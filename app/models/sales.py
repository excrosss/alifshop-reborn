import enum
from sqlalchemy import (
    Integer, String, Date, DateTime, Text, Numeric, Enum,
    ForeignKey, UniqueConstraint, func
)
from sqlalchemy.orm import Mapped, mapped_column
from app.models.base import Base

class SkuStatus(str, enum.Enum):
    ACTIVE = "active"
    MISSING = "missing"
    DELETED = "deleted"
    UNKNOWN = "unknown"

class ReportRun(Base):
    __tablename__ = "report_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    store_id: Mapped[int | None] = mapped_column(Integer, nullable=True)  # often report is for main, but keep
    report_id: Mapped[str] = mapped_column(String(64), nullable=False)    # alif report_id
    type_id: Mapped[int] = mapped_column(Integer, nullable=False)
    date_from: Mapped[Date] = mapped_column(Date, nullable=False)
    date_to: Mapped[Date] = mapped_column(Date, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="CREATED")

    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now())

class RawSalesRow(Base):
    __tablename__ = "raw_sales_rows"
    __table_args__ = (
        UniqueConstraint("report_run_id", "source_row_no", name="uq_raw_report_row"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    report_run_id: Mapped[int] = mapped_column(ForeignKey("report_runs.id", ondelete="CASCADE"), nullable=False)

    source_row_no: Mapped[int] = mapped_column(Integer, nullable=False)  # Unnamed: 0
    sale_date: Mapped[Date | None] = mapped_column(Date, nullable=True)
    application_id: Mapped[int | None] = mapped_column(Integer, nullable=True)  # Id заявки

    client: Mapped[str | None] = mapped_column(String(256), nullable=True)
    product_name: Mapped[str | None] = mapped_column(Text, nullable=True)

    price: Mapped[Numeric | None] = mapped_column(Numeric(18, 2), nullable=True)
    sku: Mapped[str | None] = mapped_column(String(64), nullable=True)

    quantity: Mapped[int] = mapped_column(Integer, nullable=False, default=1)  # in file always 1
    total: Mapped[Numeric | None] = mapped_column(Numeric(18, 2), nullable=True)

    marking: Mapped[str | None] = mapped_column(String(64), nullable=True)
    store_name: Mapped[str | None] = mapped_column(String(128), nullable=True)

    region: Mapped[str | None] = mapped_column(String(128), nullable=True)
    district: Mapped[str | None] = mapped_column(String(128), nullable=True)
    inn: Mapped[str | None] = mapped_column(String(32), nullable=True)

    period: Mapped[int | None] = mapped_column(Integer, nullable=True)
    first_payment_date: Mapped[str | None] = mapped_column(String(64), nullable=True)
    approval_date: Mapped[str | None] = mapped_column(String(64), nullable=True)

    partner_name: Mapped[str | None] = mapped_column(String(256), nullable=True)
    invoice: Mapped[str | None] = mapped_column(String(128), nullable=True)
    return_type: Mapped[str | None] = mapped_column(String(128), nullable=True)

    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now())

class SalesFact(Base):
    __tablename__ = "sales_fact"
    __table_args__ = (
        # агрегированная уникальность по группе (без source_row_no)
        UniqueConstraint(
            "store_id", "sale_date", "application_id", "sku", "price", "total", "invoice", "return_type",
            name="uq_sales_fact_group"
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    store_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    store_name: Mapped[str | None] = mapped_column(String(128), nullable=True)

    sale_date: Mapped[Date | None] = mapped_column(Date, nullable=True)
    application_id: Mapped[int | None] = mapped_column(Integer, nullable=True)

    sku: Mapped[str | None] = mapped_column(String(64), nullable=True)
    product_name_snapshot: Mapped[str | None] = mapped_column(Text, nullable=True)

    qty: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    price: Mapped[Numeric | None] = mapped_column(Numeric(18, 2), nullable=True)
    total: Mapped[Numeric | None] = mapped_column(Numeric(18, 2), nullable=True)

    invoice: Mapped[str | None] = mapped_column(String(128), nullable=True)
    return_type: Mapped[str | None] = mapped_column(String(128), nullable=True)

    status: Mapped[str] = mapped_column(String(16), nullable=False, default="active")  # active/canceled

    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now())

class SkuRegistry(Base):
    __tablename__ = "sku_registry"
    __table_args__ = (
        UniqueConstraint("store_id", "sku", name="uq_store_sku"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    store_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    sku: Mapped[str] = mapped_column(String(64), nullable=False)

    status: Mapped[SkuStatus] = mapped_column(Enum(SkuStatus), nullable=False, default=SkuStatus.UNKNOWN)

    first_seen_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    last_seen_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    last_seen_title: Mapped[str | None] = mapped_column(Text, nullable=True)
    resolved_offer_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    resolved_item_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
