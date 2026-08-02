"""The consumer side of the flow - shared by all three consumer services.

Read this file if you read only one. Everything about groups, offsets,
commits, rebalancing and at-least-once delivery lives here.
"""

import asyncio
import json
import signal
from collections.abc import Awaitable, Callable

from aiokafka import AIOKafkaConsumer, ConsumerRecord

from app.core.config import get_settings
from app.core.logging import get_logger

log = get_logger(__name__)

Handler = Callable[[dict], Awaitable[None]]


def _install_signal_handlers(stop: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)


async def run_consumer(topic: str, group_id: str, handler: Handler) -> None:
    """Consume `topic` as a member of `group_id` until SIGTERM/SIGINT.

    The important settings:

    group_id
        Kafka assigns each partition to exactly ONE consumer inside a group.
        Start a second process with the same group_id and the partitions are
        split between them (a *rebalance*). Start a fourth with only 3
        partitions and it sits idle - partition count is the parallelism cap.
        Start a process with a *different* group_id and it gets its own copy of
        every message, with its own offsets. That is fan-out.

    enable_auto_commit=False
        The default would commit your position on a timer, whether or not the
        work succeeded - so a crash could skip an event forever. We commit
        only after the handler returns, which gives **at-least-once**
        delivery: on a crash the batch is redelivered. That is precisely why
        every handler here must be idempotent.

    auto_offset_reset="earliest"
        Only applies when the group has NO committed offset yet (first run, or
        after the offsets were reset). It means "start from the beginning of
        the log", not "always replay".
    """
    settings = get_settings()

    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda b: json.loads(b.decode()),
        key_deserializer=lambda b: b.decode() if b else None,
    )

    stop = asyncio.Event()
    _install_signal_handlers(stop)

    await consumer.start()
    log.info("consumer.started", group=group_id, topic=topic)

    try:
        while not stop.is_set():
            # getmany returns {TopicPartition: [records]} - the grouping makes
            # it obvious that records arrive per partition, in partition order.
            batches = await consumer.getmany(timeout_ms=1000)

            for _tp, records in batches.items():
                for record in records:
                    await _handle(record, handler, group_id)

            if batches:
                # Commit AFTER the work, never before. The committed offset is
                # "the next offset I want", i.e. last processed + 1, and it is
                # stored by the broker in the internal __consumer_offsets topic
                # against this group_id.
                await consumer.commit()
                log.info(
                    "offsets.committed",
                    group=group_id,
                    partitions={
                        f"{tp.topic}-{tp.partition}": records[-1].offset + 1
                        for tp, records in batches.items()
                    },
                )
    finally:
        # Leaves the group cleanly, so the remaining members rebalance
        # immediately instead of waiting for the session timeout to expire.
        await consumer.stop()
        log.info("consumer.stopped", group=group_id, topic=topic)


async def _handle(record: ConsumerRecord, handler: Handler, group_id: str) -> None:
    # topic / partition / offset on every single message: the coordinates of
    # this record in the log, and the clearest window into how Kafka works.
    log.info(
        "event.received",
        group=group_id,
        topic=record.topic,
        partition=record.partition,
        offset=record.offset,
        key=record.key,
        event_type=record.value.get("event_type"),
    )
    try:
        await handler(record.value)
    except Exception:
        # Not swallowed: logged with the record's coordinates and re-raised, so
        # the offset is NOT committed and the record is redelivered on restart.
        # A production system would send it to a retry topic and then a DLQ
        # after N attempts, instead of blocking the partition forever.
        log.exception(
            "event.failed",
            group=group_id,
            topic=record.topic,
            partition=record.partition,
            offset=record.offset,
        )
        raise
