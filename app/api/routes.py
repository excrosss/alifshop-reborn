from __future__ import annotations
from datetime import date
from app.services.sales_pipeline import SalesPipelineService

from fastapi import APIRouter, Depends, UploadFile, File
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.core.db import get_db
from app.core.crypto import encrypt_str
from app.models.account import MerchantAccount, AccountType
from app.services.stores import StoresService
from app.services.sales_ingest import SalesIngestService

router = APIRouter()

@router.get("/health")
def health():
    return {"ok": True}


class AccountCreate(BaseModel):
    account_type: AccountType
    username: str = Field(..., max_length=64)
    password: str

    # для STORE-аккаунта опционально
    store_id: int | None = None
    store_name: str | None = None

class SalesReportRunRequest(BaseModel):
    type_id: int = 12
    date_from: date
    date_to: date
    poll_sec: int = 10
    timeout_sec: int = 900

@router.post("/sales/report-run")
def sales_report_run(payload: SalesReportRunRequest, db: Session = Depends(get_db)):
    svc = SalesPipelineService(db)
    return svc.run_report_and_ingest(
        type_id=payload.type_id,
        date_from=payload.date_from,
        date_to=payload.date_to,
        poll_sec=payload.poll_sec,
        timeout_sec=payload.timeout_sec,
    )


@router.post("/accounts")
def create_account(payload: AccountCreate, db: Session = Depends(get_db)):
    acc = MerchantAccount(
        account_type=payload.account_type,
        username=payload.username,
        password_enc=encrypt_str(payload.password),
        store_id=payload.store_id,
        store_name=payload.store_name,
    )
    db.add(acc)
    db.commit()
    db.refresh(acc)
    return {"id": acc.id, "account_type": acc.account_type, "username": acc.username}


@router.post("/stores/sync")
def sync_stores(db: Session = Depends(get_db)):
    svc = StoresService(db)
    return svc.sync()


@router.post("/sales/ingest")
def ingest_sales(file: UploadFile = File(...), db: Session = Depends(get_db)):
    content = file.file.read()
    svc = SalesIngestService(db)
    result = svc.ingest_excel_bytes(content)
    return result