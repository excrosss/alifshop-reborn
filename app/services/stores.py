from __future__ import annotations

import httpx
from sqlalchemy.orm import Session

from app.core.config import settings
from app.models.store import Store
from app.models.account import MerchantAccount, AccountType
from app.services.auth import TokenManager


class StoresService:
    def __init__(self, db: Session):
        self.db = db
        self.tm = TokenManager(db)

    def _api_headers(self, access_token: str) -> dict:
        return {
            "accept": "application/json, text/plain, */*",
            "apikey": settings.alif_api_key,
            "locale": settings.alif_locale,
            "authorization": f"Bearer {access_token}",
        }

    def sync(self) -> dict:
        main = (
            self.db.query(MerchantAccount)
            .filter(MerchantAccount.account_type == AccountType.MAIN)
            .order_by(MerchantAccount.id.desc())
            .first()
        )
        if not main:
            raise ValueError("MAIN аккаунт не найден. Сначала POST /accounts (account_type=main).")

        token = self.tm.ensure_access_token(main)

        url = f"{settings.alif_api_base}/merchant/merchant/stores"
        with httpx.Client(timeout=30) as client:
            r = client.get(url, headers=self._api_headers(token))
            r.raise_for_status()
            data = r.json()

        # API может вернуть list или {"data":[...]}
        stores = data.get("data") if isinstance(data, dict) else data
        if not isinstance(stores, list):
            raise RuntimeError(f"Неожиданный формат ответа stores: {type(data)}")

        upserted = 0
        for s in stores:
            sid = s.get("id") or s.get("store_id")
            name = s.get("name") or s.get("title")
            if sid is None or not name:
                continue

            obj = self.db.get(Store, int(sid))
            if obj:
                if obj.name != name:
                    obj.name = name
            else:
                self.db.add(Store(id=int(sid), name=str(name)))
            upserted += 1

        self.db.commit()
        return {"count": len(stores), "upserted": upserted}
