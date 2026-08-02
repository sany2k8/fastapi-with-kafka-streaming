"""Consumer 2 - group `decision-writer`, reads fraud.events.

    fraud.events --> UPDATE payments.status
                 --> INSERT fraud_decisions (ON CONFLICT DO NOTHING)

The single writer of the final payment state. Because the detector only
publishes and this consumer only writes, a redelivered event can never corrupt
anything: the insert is a no-op and the status update is the same value again.

Run:  python -m app.consumers.decision_writer
"""

import asyncio
import uuid

from app.core.config import get_settings
from app.core.db import SessionLocal
from app.core.logging import configure_logging, get_logger
from app.kafka.consumer import run_consumer
from app.repositories import fraud_decisions as decisions_repo
from app.repositories import payments as payments_repo
from app.schemas.events import FraudDecisionEvent

log = get_logger(__name__)


async def handle_decision(value: dict) -> None:
    event = FraudDecisionEvent.model_validate(value)

    async with SessionLocal() as session:
        written = await decisions_repo.insert_if_absent(
            session,
            decision_id=event.event_id or f"decision-{uuid.uuid4().hex[:12]}",
            payment_id=event.payment_id,
            user_id=event.user_id,
            risk_score=event.risk_score,
            risk_level=event.risk_level,
            decision=event.decision,
            reasons=event.reasons,
        )
        await payments_repo.set_status(session, event.payment_id, event.decision)

    log.info(
        "payment.status.updated",
        payment_id=event.payment_id,
        status=event.decision,
        risk_score=event.risk_score,
        # False here means this event had already been applied - a redelivery
        # that the unique constraint absorbed. Exactly what should happen.
        first_time=written,
    )


async def main() -> None:
    configure_logging()
    settings = get_settings()
    await run_consumer(
        topic=settings.topic_fraud_events,
        group_id=settings.group_decision_writer,
        handler=handle_decision,
    )


if __name__ == "__main__":
    asyncio.run(main())
