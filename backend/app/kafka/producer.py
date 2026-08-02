"""The producer side of the flow.

Producing a record means: pick a partition, append to that partition's log,
get back an offset. That is the whole story, and `publish()` logs all three so
you can watch it happen.
"""

import json
from typing import Any

from aiokafka import AIOKafkaProducer
from pydantic import BaseModel

from app.core.config import get_settings
from app.core.logging import get_logger

log = get_logger(__name__)


def _serialize(value: Any) -> bytes:
    # `default=str` handles Decimal and datetime without a custom encoder.
    return json.dumps(value, default=str).encode()


class EventProducer:
    """A single long-lived producer, shared by the whole process.

    Creating a producer per request would throw away batching and reopen a TCP
    connection every time - one of the most common Kafka mistakes.
    """

    def __init__(self) -> None:
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        settings = get_settings()
        self._producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            value_serializer=_serialize,
            key_serializer=lambda k: k.encode(),
            # acks="all": the broker only acknowledges once the record is
            # written to the leader's log (and every in-sync replica, if there
            # were any). This is the durability knob.
            acks="all",
            # Retries can duplicate a record; the idempotent producer makes the
            # broker deduplicate them, so a retry cannot silently double-write.
            enable_idempotence=True,
        )
        await self._producer.start()
        log.info("producer.started", bootstrap=settings.kafka_bootstrap_servers)

    async def stop(self) -> None:
        if self._producer is not None:
            # flush() waits for records still sitting in the batch buffer.
            await self._producer.stop()
            self._producer = None
            log.info("producer.stopped")

    async def publish(self, topic: str, key: str, event: BaseModel) -> None:
        """Publish one event.

        `key` decides the partition: partition = hash(key) % num_partitions.

        We always key by **user_id**, never payment_id. Every fraud rule asks
        "what has this user done recently?", so all of a user's events must land
        on one partition, which means one consumer in the group handles them,
        in order, one at a time. Key by payment_id instead and two payments from
        the same user get scored concurrently on different partitions - both
        read the same history and both miss the pattern.
        """
        if self._producer is None:
            raise RuntimeError("producer not started")

        metadata = await self._producer.send_and_wait(
            topic, value=event.model_dump(mode="json"), key=key
        )
        log.info(
            "event.published",
            event_type=getattr(event, "event_type", "?"),
            topic=metadata.topic,
            partition=metadata.partition,  # chosen by hash(key)
            offset=metadata.offset,  # assigned by the broker on append
            key=key,
        )


#: Module-level singleton, started in the FastAPI lifespan and in each consumer
#: that needs to produce (the detector produces to fraud.events).
producer = EventProducer()
