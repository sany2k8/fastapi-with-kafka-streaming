from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field


class PaymentCreate(BaseModel):
    user_id: str = Field(min_length=1, max_length=64, examples=["user-123"])
    amount: Decimal = Field(gt=0, max_digits=12, decimal_places=2, examples=[500.00])
    currency: str = Field(min_length=3, max_length=3, examples=["USD"])
    country: str = Field(min_length=2, max_length=2, examples=["US"])
    device_id: str = Field(min_length=1, max_length=64, examples=["device-456"])


class PaymentAccepted(BaseModel):
    """What POST /payments returns - deliberately without a risk score.

    The API has not scored anything at this point and must not wait for the
    detector to do so. "processing" is the honest answer.
    """

    payment_id: str
    status: str = "processing"


class PaymentOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    payment_id: str
    user_id: str
    amount: Decimal
    currency: str
    country: str
    device_id: str
    status: str
    created_at: datetime
    risk_score: int | None = None
    risk_level: str | None = None


class FraudDecisionOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    payment_id: str
    user_id: str
    risk_score: int
    risk_level: str
    decision: str
    reasons: list[str]
    created_at: datetime
