# app/services/auth.py

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import settings
from app.core.crypto import decrypt_str, encrypt_str
from app.models.account import MerchantAccount


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _is_valid(expires_at: Optional[datetime], skew_sec: int = 60) -> bool:
    if not expires_at:
        return False
    return expires_at > (_utcnow() + timedelta(seconds=skew_sec))


class AuthService:
    """
    Token Manager:
    - password flow (grant_type=password)
    - refresh flow (grant_type=refresh_token)
    - хранит access_token + expires_at
    - refresh_token хранит ЗАШИФРОВАННЫМ (refresh_token_enc)
    """

    def __init__(self, db: Session):
        self.db = db

    def get_valid_access_token(self, account_id: int) -> str:
        acc = self.db.execute(
            select(MerchantAccount).where(MerchantAccount.id == account_id)
        ).scalar_one()

        # 1) access токен ещё живой
        if acc.access_token and _is_valid(acc.access_expires_at):
            return acc.access_token

        # 2) пробуем refresh
        if acc.refresh_token_enc:
            refreshed = self._refresh(acc)
            if refreshed:
                return refreshed

        # 3) иначе password flow
        return self._password(acc)

    # ---------- flows ----------

    def _password(self, acc: MerchantAccount) -> str:
        username = acc.username
        password = decrypt_str(acc.password_enc)

        data = {
            "client_id": settings.alif_client_id,
            "username": username,
            "password": password,
            "grant_type": "password",
            "scope": "openid",
        }

        with httpx.Client(timeout=60) as client:
            r = client.post(
                settings.alif_auth_url,
                data=data,  # form-urlencoded
                headers={"accept": "application/json"},
            )
            r.raise_for_status()
            payload = r.json()

        self._apply_token_response(acc, payload)
        self.db.add(acc)
        self.db.commit()
        self.db.refresh(acc)

        if not acc.access_token:
            raise RuntimeError(f"Не получили access_token. Ответ: {payload}")
        return acc.access_token

    def _refresh(self, acc: MerchantAccount) -> Optional[str]:
        try:
            refresh_token = decrypt_str(acc.refresh_token_enc) if acc.refresh_token_enc else None
        except Exception:
            refresh_token = None

        if not refresh_token:
            return None

        data = {
            "client_id": settings.alif_client_id,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        }

        try:
            with httpx.Client(timeout=60) as client:
                r = client.post(
                    settings.alif_auth_url,
                    data=data,
                    headers={"accept": "application/json"},
                )
                r.raise_for_status()
                payload = r.json()
        except Exception:
            return None

        self._apply_token_response(acc, payload)
        self.db.add(acc)
        self.db.commit()
        self.db.refresh(acc)

        return acc.access_token

    # ---------- helpers ----------

    def _apply_token_response(self, acc: MerchantAccount, payload: dict) -> None:
        access_token = payload.get("access_token")
        refresh_token = payload.get("refresh_token")
        expires_in = int(payload.get("expires_in") or 3600)

        if access_token:
            acc.access_token = access_token
            acc.access_expires_at = _utcnow() + timedelta(seconds=expires_in - 60)

        # refresh у keycloak часто обновляется — сохраняем новый, если пришёл
        if refresh_token:
            acc.refresh_token_enc = encrypt_str(refresh_token)

        # refresh_expires_at обычно не дают — оставим None (или можно хранить “как есть”)
        acc.refresh_expires_at = acc.refresh_expires_at
