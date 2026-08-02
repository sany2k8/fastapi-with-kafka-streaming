"""The two event shapes that travel through Kafka.

These are the contract between the producer and the consumers. Everything else
in the system is an implementation detail of one side or the other.
"""

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, Field


def _now() -> datetime:
    from datetime import UTC

    return datetime.now(UTC)


class PaymentCreatedEvent(BaseModel):
    """Published to `payment.events` by the API, keyed by user_id."""

    event_id: str
    event_type: str = "payment.created"
    timestamp: datetime = Field(default_factory=_now)

    payment_id: str
    user_id: str
    amount: Decimal
    currency: str
    country: str
    device_id: str


class FraudDecisionEvent(BaseModel):
    """Published to `fraud.events` by the detector, keyed by user_id.

    `event_type` is `fraud.detected` when the payment is blocked and
    `payment.approved` otherwise - one topic carrying two event types, which is
    why consumers must branch on `event_type` rather than assume.
    """

    event_id: str
    event_type: str
    timestamp: datetime = Field(default_factory=_now)

    payment_id: str
    user_id: str
    risk_score: int
    risk_level: str
    decision: str
    reasons: list[str] = Field(default_factory=list)
