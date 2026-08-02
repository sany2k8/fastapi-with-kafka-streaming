from datetime import datetime
from decimal import Decimal

from sqlalchemy import Index, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, utc_now_column

#: processing -> the API accepted it, the detector has not decided yet
#: approved   -> risk score below the threshold
#: blocked    -> risk score at or above the threshold
PaymentStatus = str


class Payment(Base):
    __tablename__ = "payments"

    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    user_id: Mapped[str] = mapped_column(String(64), nullable=False)
    amount: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    country: Mapped[str] = mapped_column(String(2), nullable=False)
    device_id: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[PaymentStatus] = mapped_column(String(16), nullable=False, default="processing")
    created_at: Mapped[datetime] = utc_now_column()

    # Every fraud rule is "what did this user do recently?", so this composite
    # index is the difference between a fast detector and a table scan per event.
    __table_args__ = (Index("ix_payments_user_created", "user_id", "created_at"),)
