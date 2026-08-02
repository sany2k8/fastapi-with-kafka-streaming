#!/usr/bin/env bash
# Rewind a consumer group to the beginning of the log and let it reprocess.
#
# This is the property that makes Kafka a log rather than a queue: the records
# are still there. Consuming them did not remove them - it only moved this
# group's committed offset. Move the offset back and the events replay.
#
# We rewind `audit-logger` because it only writes log lines, so a replay is
# free. Rewinding `decision-writer` would be safe too - its insert is
# ON CONFLICT DO NOTHING - which is the whole point of making it idempotent.
set -euo pipefail

GROUP="${1:-audit-logger}"
TOPIC="${2:-fraud.events}"

echo "Stopping the $GROUP consumer..."
# A group's offsets can only be changed while the group has no active members.
docker compose stop "$GROUP" >/dev/null

echo "Resetting $GROUP to the earliest offset on $TOPIC..."
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group "$GROUP" \
  --topic "$TOPIC" \
  --reset-offsets --to-earliest --execute

echo
echo "Starting it again - it will now reprocess every fraud event ever published."
docker compose start "$GROUP" >/dev/null
sleep 6
docker compose logs --tail 5 "$GROUP"

echo
echo "Note that decision-writer was completely unaffected: separate group,"
echo "separate offsets. Check with:  make inspect"
