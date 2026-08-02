"""Runs every rule against one payment event and produces an assessment."""

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.core.logging import get_logger
from app.fraud.risk_engine import RiskAssessment, assess
from app.fraud.rules import ALL_RULES, RuleHit
from app.repositories import payments as payments_repo
from app.schemas.events import PaymentCreatedEvent

log = get_logger(__name__)


async def evaluate(session: AsyncSession, event: PaymentCreatedEvent) -> RiskAssessment:
    settings = get_settings()

    # The payment row is the source of truth for *when* this happened, and
    # every history query is anchored to it. Falling back to the event's own
    # timestamp keeps the detector working even if the row is somehow missing.
    payment = await payments_repo.get(session, event.payment_id)
    at = payment.created_at if payment is not None else event.timestamp

    hits: list[RuleHit] = []
    for rule in ALL_RULES:
        hit = await rule(session, event, at, settings)
        if hit is not None:
            hits.append(hit)

    assessment = assess(hits, fraud_threshold=settings.fraud_threshold)

    log.info(
        "risk.assessed",
        payment_id=event.payment_id,
        user_id=event.user_id,
        risk_score=assessment.risk_score,
        risk_level=assessment.risk_level,
        decision=assessment.decision,
        reasons=assessment.reasons,
    )
    return assessment
