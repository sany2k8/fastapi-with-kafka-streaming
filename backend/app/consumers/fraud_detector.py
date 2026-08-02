"""Consumer 1 - group `fraud-detector`, reads payment.events.

    payment.events --> [load history] --> [5 rules] --> [score] --> fraud.events

It never writes the payment status. Its only outputs are a Kafka event and a
log line, which keeps the write path in exactly one place (decision_writer).

Run:      python -m app.consumers.fraud_detector
Scale:    docker compose up -d --scale fraud-detector=3
"""

import asyncio
import uuid

from app.core.config import get_settings
from app.core.db import SessionLocal
from app.core.logging import configure_logging, get_logger
from app.fraud.detector import evaluate
from app.kafka.consumer import run_consumer
from app.kafka.producer import producer
from app.schemas.events import FraudDecisionEvent, PaymentCreatedEvent

log = get_logger(__name__)


async def handle_payment_created(value: dict) -> None:
    settings = get_settings()
    event = PaymentCreatedEvent.model_validate(value)

    async with SessionLocal() as session:
        assessment = await evaluate(session, event)

    decision_event = FraudDecisionEvent(
        event_id=f"decision-{uuid.uuid4().hex[:12]}",
        event_type=assessment.event_type,
        payment_id=event.payment_id,
        user_id=event.user_id,
        risk_score=assessment.risk_score,
        risk_level=assessment.risk_level,
        decision=assessment.decision,
        reasons=assessment.reasons,
    )

    # Keyed by user_id again, so a user's decisions stay ordered relative to
    # each other on the downstream topic too.
    await producer.publish(settings.topic_fraud_events, key=event.user_id, event=decision_event)


async def main() -> None:
    configure_logging()
    settings = get_settings()

    await producer.start()
    try:
        await run_consumer(
            topic=settings.topic_payment_events,
            group_id=settings.group_fraud_detector,
            handler=handle_payment_created,
        )
    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
