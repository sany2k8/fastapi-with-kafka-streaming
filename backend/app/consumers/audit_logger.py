"""Consumer 3 - group `audit-logger`, reads fraud.events. Same topic as
decision_writer, different group.

This service exists to make one thing undeniable: two consumer groups
subscribed to the same topic BOTH receive every message, and each tracks its
own offsets. That is the difference between Kafka and a work queue - in a
queue, whichever consumer grabbed the message first would be the only one to
see it.

Stop this service for a while, then start it again: it resumes from its own
committed offset and catches up, while decision-writer was never affected.

Run:  python -m app.consumers.audit_logger
"""

import asyncio

from app.core.config import get_settings
from app.core.logging import configure_logging, get_logger
from app.kafka.consumer import run_consumer
from app.schemas.events import FraudDecisionEvent

log = get_logger(__name__)


async def handle_decision(value: dict) -> None:
    event = FraudDecisionEvent.model_validate(value)
    log.info(
        "audit.decision",
        event_type=event.event_type,
        payment_id=event.payment_id,
        user_id=event.user_id,
        risk_score=event.risk_score,
        risk_level=event.risk_level,
        reasons=event.reasons,
    )


async def main() -> None:
    configure_logging()
    settings = get_settings()
    await run_consumer(
        topic=settings.topic_fraud_events,
        group_id=settings.group_audit_logger,
        handler=handle_decision,
    )


if __name__ == "__main__":
    asyncio.run(main())
