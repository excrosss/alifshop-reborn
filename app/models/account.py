import enum
from sqlalchemy import String, Integer, Enum, DateTime, func, Text
from sqlalchemy.orm import Mapped, mapped_column
from app.models.base import Base

class AccountType(str, enum.Enum):
    MAIN = "main"
    STORE = "store"

class MerchantAccount(Base):
    __tablename__ = "merchant_accounts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    account_type: Mapped[AccountType] = mapped_column(Enum(AccountType), nullable=False)

    # login
    username: Mapped[str] = mapped_column(String(64), nullable=False)
    password_enc: Mapped[str] = mapped_column(Text, nullable=False)

    # tokens (refresh can live long)
    access_token: Mapped[str | None] = mapped_column(Text, nullable=True)
    access_expires_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    refresh_token_enc: Mapped[str | None] = mapped_column(Text, nullable=True)
    refresh_expires_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # optional binding (for store accounts)
    store_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    store_name: Mapped[str | None] = mapped_column(String(128), nullable=True)

    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
