# app/services/sales_reports.py

from __future__ import annotations

import time
from datetime import date
import httpx
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import settings
from app.models.account import MerchantAccount, AccountType
from app.services.auth import AuthService


API_BASE = "https://api-merchant.alif.uz/merchant/excel/excel/v1/reports"


class SalesReportsService:
    def __init__(self, db: Session):
        self.db = db
        self.auth = AuthService(db)

    def _main_account(self) -> MerchantAccount:
        acc = self.db.execute(
            select(MerchantAccount).where(MerchantAccount.account_type == AccountType.MAIN)
        ).scalar_one_or_none()
        if not acc:
            raise ValueError("MAIN аккаунт не найден. Сначала добавь его через POST /accounts.")
        return acc

    def _headers(self) -> dict:
        token = self.auth.get_valid_access_token(self._main_account().id)
        return {
            "accept": "application/json, text/plain, */*",
            "content-type": "application/json",
            "apikey": settings.alif_api_key,
            "locale": settings.alif_locale,
            "authorization": f"Bearer {token}",
        }

    def generate(self, type_id: int, date_from: date, date_to: date) -> str:
        payload = {
            "type_id": int(type_id),
            "datetime_from": str(date_from),
            "datetime_to": str(date_to),
        }
        with httpx.Client(timeout=60) as client:
            r = client.post(f"{API_BASE}/generate", headers=self._headers(), json=payload)
            r.raise_for_status()
            data = r.json()
        report_id = data.get("report_id")
        if not report_id:
            raise RuntimeError(f"Не получили report_id. Ответ: {data}")
        return str(report_id)

    def check(self, report_id: str) -> str:
        with httpx.Client(timeout=60) as client:
            r = client.get(f"{API_BASE}/check", headers=self._headers(), params={"report_id": report_id})
            r.raise_for_status()
            data = r.json()
        return str(data.get("status") or "UNKNOWN")

    def wait_success(self, report_id: str, poll_sec: int = 10, timeout_sec: int = 900) -> None:
        start = time.time()
        while True:
            st = self.check(report_id)
            if st == "SUCCESS":
                return
            if st == "FAILED":
                raise RuntimeError(f"Отчёт {report_id} FAILED")
            if time.time() - start > timeout_sec:
                raise TimeoutError(f"Таймаут ожидания отчёта {report_id} ({timeout_sec}s)")
            time.sleep(poll_sec)

    def download_bytes(self, report_id: str) -> bytes:
        headers = self._headers()
        headers["accept"] = "*/*"
        with httpx.Client(timeout=180) as client:
            r = client.get(f"{API_BASE}/download", headers=headers, params={"report_id": report_id})
            r.raise_for_status()
            return r.content
