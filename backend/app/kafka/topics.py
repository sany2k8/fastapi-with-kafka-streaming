"""Explicit topic creation.

Auto-creation is disabled on the broker on purpose. If a topic were created
implicitly by the first producer it would get the broker default of 1
partition, and you would never see a rebalance or parallel consumers.
"""

from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError

from app.core.config import get_settings
from app.core.logging import get_logger

log = get_logger(__name__)


async def ensure_topics() -> None:
    settings = get_settings()
    admin = AIOKafkaAdminClient(bootstrap_servers=settings.kafka_bootstrap_servers)
    await admin.start()
    try:
        new_topics = [
            NewTopic(
                name=name,
                num_partitions=settings.topic_partitions,
                replication_factor=1,  # single broker - nothing to replicate to
            )
            for name in settings.topics
        ]
        try:
            await admin.create_topics(new_topics)
            log.info(
                "kafka.topics.created",
                topics=settings.topics,
                partitions=settings.topic_partitions,
            )
        except TopicAlreadyExistsError:
            log.info("kafka.topics.exist", topics=settings.topics)
    finally:
        await admin.close()
