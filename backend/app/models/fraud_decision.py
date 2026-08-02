from datetime import datetime

from sqlalchemy import JSON, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, utc_now_column


class FraudDecision(Base):
    __tablename__ = "fraud_decisions"

    id: Mapped[str] = mapped_column(String(40), primary_key=True)

    # UNIQUE is the idempotency guard. Kafka is at-least-once: on a rebalance
    # or a crash before commit, the same payment.created is redelivered. The
    # writer does INSERT ... ON CONFLICT DO NOTHING, so a replay is a no-op
    # instead of a duplicate row. No Redis needed - Postgres is the dedup store.
    payment_id: Mapped[str] = mapped_column(
        String(40), ForeignKey("payments.id"), unique=True, nullable=False
    )
    user_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    risk_score: Mapped[int] = mapped_column(Integer, nullable=False)
    risk_level: Mapped[str] = mapped_column(String(10), nullable=False)
    decision: Mapped[str] = mapped_column(String(16), nullable=False)
    reasons: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)
    created_at: Mapped[datetime] = utc_now_column()
