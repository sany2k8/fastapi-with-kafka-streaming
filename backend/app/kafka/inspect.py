"""Reads Kafka's own bookkeeping so offsets, ownership and lag become numbers.

Lag is the single most useful Kafka metric in production:

    lag = end_offset (last record written) - committed_offset (group's position)

Zero lag means the group is caught up. Growing lag means the consumers cannot
keep up with the producers - the thing you page on.
"""

from typing import Any

from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient
from aiokafka.coordinator.protocol import ConsumerProtocolMemberAssignment

from app.core.config import get_settings


async def _topic_partitions(admin: AIOKafkaAdminClient, topic: str) -> list[int]:
    """Partition ids for a topic.

    Deliberately via the admin client: a consumer only caches metadata for
    topics it is subscribed to, so `consumer.partitions_for_topic()` returns
    None here even after `await consumer.topics()`.
    """
    described = await admin.describe_topics([topic])
    if not described:
        return []
    return sorted(p["partition"] for p in described[0]["partitions"])


async def _describe_group(admin: AIOKafkaAdminClient, group_id: str) -> dict[str, Any]:
    """Group state plus which member currently owns which partition.

    Run `make scale` and refresh: one member holding [0, 1, 2] becomes three
    members holding one partition each. That is a rebalance, visible.
    """
    response = await admin.describe_consumer_groups([group_id])
    if not response or not response[0].groups:
        return {"state": "Unknown", "members": []}

    _err, _gid, state, _proto_type, _proto, raw_members = response[0].groups[0]

    members = []
    for member_id, _client_id, client_host, _metadata, raw_assignment in raw_members:
        assignment = ConsumerProtocolMemberAssignment.decode(raw_assignment)
        members.append(
            {
                "member_id": member_id,
                "client_host": client_host,
                "assigned_partitions": {
                    topic: partitions for topic, partitions in assignment.assignment
                },
            }
        )
    return {"state": state, "member_count": len(members), "members": members}


async def inspect_cluster() -> dict[str, Any]:
    settings = get_settings()

    admin = AIOKafkaAdminClient(bootstrap_servers=settings.kafka_bootstrap_servers)
    # No group_id, so this consumer never joins a group and cannot take
    # partitions away from a real consumer. It only reads offsets.
    probe = AIOKafkaConsumer(bootstrap_servers=settings.kafka_bootstrap_servers)

    await admin.start()
    await probe.start()
    try:
        end_offsets: dict[TopicPartition, int] = {}
        topics: dict[str, Any] = {}

        for topic in settings.topics:
            partition_ids = await _topic_partitions(admin, topic)
            tps = [TopicPartition(topic, p) for p in partition_ids]
            if tps:
                end_offsets.update(await probe.end_offsets(tps))

            topics[topic] = {
                "partition_count": len(partition_ids),
                "partitions": [
                    {
                        "partition": tp.partition,
                        # end_offset == how many records this partition has ever
                        # held: offsets start at 0 and only move forwards.
                        "end_offset": end_offsets.get(tp, 0),
                    }
                    for tp in tps
                ],
                "total_records": sum(end_offsets.get(tp, 0) for tp in tps),
            }

        groups: dict[str, Any] = {}
        for group_id in settings.consumer_groups:
            committed = await admin.list_consumer_group_offsets(group_id)
            rows = []
            total_lag = 0
            for tp, meta in sorted(
                committed.items(), key=lambda kv: (kv[0].topic, kv[0].partition)
            ):
                end = end_offsets.get(tp, 0)
                # -1 means this group has never committed on this partition.
                position = meta.offset if meta.offset >= 0 else 0
                lag = max(end - position, 0)
                total_lag += lag
                rows.append(
                    {
                        "topic": tp.topic,
                        "partition": tp.partition,
                        "committed_offset": meta.offset,
                        "end_offset": end,
                        "lag": lag,
                    }
                )
            groups[group_id] = {
                **await _describe_group(admin, group_id),
                "total_lag": total_lag,
                "offsets": rows,
            }

        return {
            "bootstrap_servers": settings.kafka_bootstrap_servers,
            "topics": topics,
            "consumer_groups": groups,
            "hints": [
                "lag = end_offset - committed_offset",
                "Records are keyed by user_id, so one busy user concentrates on a "
                "single partition - expect an uneven spread, that is the trade-off "
                "you accept in exchange for per-user ordering.",
                "decision-writer and audit-logger read the SAME topic in different "
                "groups: both see every record, with independent offsets.",
                "Run `make scale` and refresh to watch 3 partitions redistribute "
                "across 3 members of the fraud-detector group.",
            ],
        }
    finally:
        await probe.stop()
        await admin.close()
