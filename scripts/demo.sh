#!/usr/bin/env bash
# Guided walkthrough of the whole flow. Run it with `make demo`.
set -euo pipefail

API="${API:-http://localhost:8700}"
HERE="$(dirname "$0")"
USER_ID="demo-$RANDOM"

hr()   { printf '\n\033[1m%s\033[0m\n%s\n' "$1" "$(printf '=%.0s' {1..70})"; }
note() { printf '  \033[2m%s\033[0m\n' "$1"; }

hr "1. A normal payment"
note "POST /payments writes the row, publishes payment.created, returns 202."
note "It does NOT score anything - watch how fast it returns."

START=$(python3 -c 'import time;print(time.time())')
PAYMENT=$(curl -s -X POST "$API/payments" -H 'content-type: application/json' \
  -d "{\"user_id\":\"$USER_ID\",\"amount\":50.00,\"currency\":\"USD\",\"country\":\"US\",\"device_id\":\"device-1\"}")
END=$(python3 -c 'import time;print(time.time())')
PAYMENT_ID=$(echo "$PAYMENT" | python3 -c 'import sys,json;print(json.load(sys.stdin)["payment_id"])')

echo "$PAYMENT" | python3 -m json.tool
python3 -c "print('  API responded in %.0f ms' % ((float('$END')-float('$START'))*1000))"

hr "2. The asynchronous gap"
note "The response above says 'processing' - the API has no risk score to give."
note "Polling until the pipeline settles:"

SETTLE_START=$(python3 -c 'import time;print(time.time())')
for _ in $(seq 1 40); do
  STATUS=$(curl -s "$API/payments/$PAYMENT_ID" | python3 -c 'import sys,json;print(json.load(sys.stdin)["status"])')
  NOW=$(python3 -c 'import time;print(time.time())')
  python3 -c "print('    +%4.0f ms  status=%s' % ((float('$NOW')-float('$SETTLE_START'))*1000, '$STATUS'))"
  [ "$STATUS" != "processing" ] && break
  sleep 0.1
done

note "That settle time is the price of asynchrony - and the reason POST stayed fast."
note "It is also the whole trade-off: the client learns the outcome later."
curl -s "$API/payments/$PAYMENT_ID/fraud" | python3 -m json.tool

hr "3. Where that payment physically lives in Kafka"
note "Keyed by user_id, so this user's events all sit on one partition."
docker compose logs api 2>/dev/null | grep 'event.published' | tail -1 || true

hr "4. An account takeover"
note "25 payments in 30s; from #20 the country and device change on every one."
"$HERE/burst.sh" "$USER_ID-takeover"

hr "5. Fan-out: two groups, same topic"
note "decision-writer wrote the status. audit-logger saw the identical events"
note "in its own group, with its own offsets, and only logged them:"
docker compose logs audit-logger 2>/dev/null | grep 'audit.decision' | tail -3 || true

hr "6. Offsets and lag"
curl -s "$API/kafka/inspect" | python3 "$HERE/_show.py" kafka

hr "Done"
note "Try next:"
note "  make scale     -> 3 detectors, watch 3 partitions redistribute"
note "  ./scripts/replay.sh -> rewind audit-logger's offsets and reprocess history"
note "  make ui        -> Kafka UI on http://localhost:8092"
