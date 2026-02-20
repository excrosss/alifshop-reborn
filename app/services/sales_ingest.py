# app/services/sales_ingest.py

from __future__ import annotations

import io
import re
from datetime import date
from typing import Any

import pandas as pd
from sqlalchemy import select, func
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from app.models.sales import RawSalesRow, SalesFact, SkuRegistry, SkuStatus


def _norm_sku(v: Any) -> str | None:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v).strip()
    if not s:
        return None
    s = re.sub(r"[^0-9]", "", s)
    return s or None


def _to_date(v: Any) -> date | None:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    ts = pd.to_datetime(v, errors="coerce", dayfirst=True)
    if pd.isna(ts):
        return None
    return ts.date()


def _safe_int(v: Any) -> int | None:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    try:
        return int(str(v).replace(" ", "").replace("\xa0", "").replace(",", ""))
    except Exception:
        return None


def _safe_num(v: Any) -> float | None:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v).replace(" ", "").replace("\xa0", "").replace(",", "")
    try:
        return float(s)
    except Exception:
        return None


class SalesIngestService:
    """
    Делает:
    1) читает merchants.xlsx (учитывая пустой 1й столбец)
    2) пишет raw_sales_rows (on_conflict_do_nothing)
    3) аггрегирует raw -> sales_fact (qty = count)
    4) обновляет sku_registry (first/last seen)
    """

    # ожидаемые колонки ПОСЛЕ первого столбца
    EXPECTED_COLS = [
        "sale_date",
        "application_id",
        "client",
        "product_name",
        "price",
        "sku",
        "quantity",
        "total",
        "marking",
        "store_name",
        "region",
        "district",
        "inn",
        "period",
        "first_payment_date",
        "approval_date",
        "partner_name",
        "invoice",
        "return_type",
    ]

    def ingest_excel_bytes(
        self,
        db: Session,
        report_run_id: int,
        excel_bytes: bytes,
        store_id: int | None = None,
    ) -> dict:
        df = self._read_excel(excel_bytes)

        raw_rows = self._build_raw_rows(report_run_id=report_run_id, df=df)
        inserted_raw = self._insert_raw(db, raw_rows)

        # берем ВСЕ raw для этого report_run_id (включая уже существующие)
        raw_df = self._load_raw_df(db, report_run_id)

        fact_rows = self._build_fact_rows(raw_df, store_id=store_id)
        upserted_fact = self._upsert_sales_fact(db, fact_rows)

        sku_rows = self._build_sku_registry_rows(raw_df, store_id=store_id)
        upserted_sku = self._upsert_sku_registry(db, sku_rows)

        db.commit()

        return {
            "report_run_id": int(report_run_id),
            "raw_in_file": int(len(df)),
            "raw_inserted": int(inserted_raw),
            "fact_groups": int(len(fact_rows)),
            "fact_upserted": int(upserted_fact),
            "sku_upserted": int(upserted_sku),
        }

    # ---------- Excel ----------

    def _read_excel(self, excel_bytes: bytes) -> pd.DataFrame:
        df = pd.read_excel(io.BytesIO(excel_bytes), sheet_name=0)

        # первый столбец в merchants.xlsx пустой по названию -> обычно "Unnamed: 0"
        # НЕ удаляем его, а используем как source_row_no
        first_col = df.columns[0]
        df = df.rename(columns={first_col: "source_row_no"}).copy()

        # ожидаем: source_row_no + 19 колонок = 20
        if len(df.columns) != 20:
            raise ValueError(
                f"Ожидалось 20 столбцов (source_row_no + 19), получено {len(df.columns)}. "
                f"Колонки: {list(df.columns)}"
            )

        df.columns = ["source_row_no"] + self.EXPECTED_COLS

        df["source_row_no"] = df["source_row_no"].apply(lambda x: _safe_int(x) or 0)

        df["sku"] = df["sku"].apply(_norm_sku)
        df["quantity"] = df["quantity"].apply(lambda x: _safe_int(x) or 1)

        return df

    # ---------- RAW ----------

    def _build_raw_rows(self, report_run_id: int, df: pd.DataFrame) -> list[dict]:
        rows: list[dict] = []
        for _, r in df.iterrows():
            rows.append(
                {
                    "report_run_id": report_run_id,
                    "source_row_no": int(r.get("source_row_no") or 0),

                    "sale_date": _to_date(r.get("sale_date")),
                    "application_id": _safe_int(r.get("application_id")),

                    "client": (None if pd.isna(r.get("client")) else str(r.get("client")).strip()),
                    "product_name": (None if pd.isna(r.get("product_name")) else str(r.get("product_name")).strip()),

                    "price": _safe_num(r.get("price")),
                    "sku": _norm_sku(r.get("sku")),

                    "quantity": _safe_int(r.get("quantity")) or 1,
                    "total": _safe_num(r.get("total")),

                    "marking": (None if pd.isna(r.get("marking")) else str(r.get("marking")).strip()),
                    "store_name": (None if pd.isna(r.get("store_name")) else str(r.get("store_name")).strip()),

                    "region": (None if pd.isna(r.get("region")) else str(r.get("region")).strip()),
                    "district": (None if pd.isna(r.get("district")) else str(r.get("district")).strip()),
                    "inn": (None if pd.isna(r.get("inn")) else str(r.get("inn")).strip()),

                    "period": _safe_int(r.get("period")),
                    "first_payment_date": (None if pd.isna(r.get("first_payment_date")) else str(r.get("first_payment_date")).strip()),
                    "approval_date": (None if pd.isna(r.get("approval_date")) else str(r.get("approval_date")).strip()),

                    "partner_name": (None if pd.isna(r.get("partner_name")) else str(r.get("partner_name")).strip()),
                    "invoice": (None if pd.isna(r.get("invoice")) else str(r.get("invoice")).strip()),
                    "return_type": (None if pd.isna(r.get("return_type")) else str(r.get("return_type")).strip()),
                }
            )
        return rows

    def _insert_raw(self, db: Session, rows: list[dict]) -> int:
        if not rows:
            return 0

        stmt = pg_insert(RawSalesRow).values(rows)
        stmt = stmt.on_conflict_do_nothing(constraint="uq_raw_report_row")

        res = db.execute(stmt)
        return res.rowcount or 0

    def _load_raw_df(self, db: Session, report_run_id: int) -> pd.DataFrame:
        q = select(RawSalesRow).where(RawSalesRow.report_run_id == report_run_id)
        raw = db.execute(q).scalars().all()

        data = []
        for r in raw:
            data.append(
                {
                    "store_name": r.store_name,
                    "sale_date": r.sale_date,
                    "application_id": r.application_id,
                    "sku": r.sku,
                    "price": float(r.price) if r.price is not None else None,
                    "total": float(r.total) if r.total is not None else None,
                    "invoice": r.invoice,
                    "return_type": r.return_type,
                    "product_name": r.product_name,
                    "source_row_no": r.source_row_no,
                }
            )
        return pd.DataFrame(data)

    # ---------- FACT ----------

    def _build_fact_rows(self, raw_df: pd.DataFrame, store_id: int | None) -> list[dict]:
        if raw_df.empty:
            return []

        def _status(row):
            return "canceled" if (row.get("invoice") == "Минусовая" or row.get("return_type") == "Полный") else "active"

        raw_df["status"] = raw_df.apply(_status, axis=1)

        group_cols = [
            "store_name", "sale_date", "application_id", "sku", "price", "total", "invoice", "return_type", "status"
        ]

        g = (
            raw_df
            .groupby(group_cols, dropna=False)
            .agg(
                qty=("source_row_no", "count"),
                product_name_snapshot=("product_name", "last"),
            )
            .reset_index()
        )

        rows: list[dict] = []
        for _, r in g.iterrows():
            rows.append(
                {
                    "store_id": store_id,
                    "store_name": None if pd.isna(r["store_name"]) else str(r["store_name"]),
                    "sale_date": None if pd.isna(r["sale_date"]) else r["sale_date"],
                    "application_id": None if pd.isna(r["application_id"]) else int(r["application_id"]),
                    "sku": None if pd.isna(r["sku"]) else str(r["sku"]),
                    "product_name_snapshot": None if pd.isna(r["product_name_snapshot"]) else str(r["product_name_snapshot"]),
                    "qty": int(r["qty"]),
                    "price": None if pd.isna(r["price"]) else float(r["price"]),
                    "total": None if pd.isna(r["total"]) else float(r["total"]),
                    "invoice": None if pd.isna(r["invoice"]) else str(r["invoice"]),
                    "return_type": None if pd.isna(r["return_type"]) else str(r["return_type"]),
                    "status": str(r["status"]),
                }
            )
        return rows

    def _upsert_sales_fact(self, db: Session, rows: list[dict]) -> int:
        if not rows:
            return 0

        stmt = pg_insert(SalesFact).values(rows)
        stmt = stmt.on_conflict_do_update(
            constraint="uq_sales_fact_group",
            set_={
                "qty": stmt.excluded.qty,  # идемпотентно
                "product_name_snapshot": stmt.excluded.product_name_snapshot,
                "status": stmt.excluded.status,
            },
        )

        res = db.execute(stmt)
        return res.rowcount or 0

    # ---------- SKU REGISTRY ----------

    def _build_sku_registry_rows(self, raw_df: pd.DataFrame, store_id: int | None) -> list[dict]:
        if raw_df.empty:
            return []

        g = (
            raw_df.dropna(subset=["sku"])
            .groupby(["sku"], dropna=False)
            .agg(last_seen_title=("product_name", "last"))
            .reset_index()
        )

        rows: list[dict] = []
        for _, r in g.iterrows():
            sku = str(r["sku"]).strip()
            if not sku:
                continue
            rows.append(
                {
                    "store_id": store_id,
                    "sku": sku,
                    "status": SkuStatus.UNKNOWN,
                    "last_seen_title": None if pd.isna(r["last_seen_title"]) else str(r["last_seen_title"]),
                }
            )
        return rows

    def _upsert_sku_registry(self, db: Session, rows: list[dict]) -> int:
        if not rows:
            return 0

        stmt = pg_insert(SkuRegistry).values(rows)
        stmt = stmt.on_conflict_do_update(
            constraint="uq_store_sku",
            set_={
                "last_seen_title": stmt.excluded.last_seen_title,
                "last_seen_at": func.now(),  # ВАЖНО: реально обновляем
            },
        )
        res = db.execute(stmt)
        return res.rowcount or 0
