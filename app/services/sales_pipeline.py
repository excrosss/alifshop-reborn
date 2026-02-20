# app/services/sales_pipeline.py

from __future__ import annotations

from datetime import date
from sqlalchemy.orm import Session

from app.models.sales import ReportRun
from app.services.sales_reports import SalesReportsService
from app.services.sales_ingest import SalesIngestService


class SalesPipelineService:
    """
    Полный пайплайн:
    1) generate report (Alif)
    2) wait SUCCESS
    3) download xlsx bytes
    4) ingest bytes -> raw_sales_rows -> sales_fact + sku_registry
    """

    def __init__(self, db: Session):
        self.db = db
        self.reports = SalesReportsService(db)
        self.ingest = SalesIngestService()  # ВАЖНО: без аргументов

    def run_report_and_ingest(
        self,
        type_id: int,
        date_from: date,
        date_to: date,
        poll_sec: int = 10,
        timeout_sec: int = 900,
    ) -> dict:
        # 1) generate
        report_id = self.reports.generate(type_id=type_id, date_from=date_from, date_to=date_to)

        rr = ReportRun(
            store_id=None,
            report_id=report_id,
            type_id=type_id,
            date_from=date_from,
            date_to=date_to,
            status="CREATED",
        )
        self.db.add(rr)
        self.db.commit()
        self.db.refresh(rr)

        # 2) wait
        rr.status = "PENDING"
        self.db.commit()

        self.reports.wait_success(report_id=report_id, poll_sec=poll_sec, timeout_sec=timeout_sec)

        rr.status = "SUCCESS"
        self.db.commit()

        # 3) download
        content = self.reports.download_bytes(report_id=report_id)

        # 4) ingest в тот же ReportRun
        rr.status = "INGESTING"
        self.db.commit()

        ingest_result = self.ingest.ingest_excel_bytes(
            db=self.db,
            report_run_id=rr.id,
            excel_bytes=content,
            store_id=None,
        )

        rr.status = "INGESTED"
        self.db.commit()

        return {
            "generated_report_run_id": rr.id,
            "alif_report_id": report_id,
            "date_from": str(date_from),
            "date_to": str(date_to),
            "ingest": ingest_result,
        }
