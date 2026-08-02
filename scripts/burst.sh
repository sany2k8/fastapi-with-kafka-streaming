#!/usr/bin/env bash
# Simulates an account takeover: a normal-looking user suddenly bursts.
#
#   #1-#19   same country, same device      -> 0,  LOW,    approved
#   #20-#25  20-in-30s crossed, and each one hops country and device:
#              frequency 40 + country 30 + device 10 = 80, HIGH, BLOCKED
#            once 5 of them are blocked, rule 3 adds 20 more -> 100
#
# The interesting part is #20: nothing about that payment is unusual on its
# own. It is only fraud in the context of the 19 events before it - which is
# exactly the kind of decision you cannot make inside a request handler.
set -euo pipefail

API="${API:-http://localhost:8700}"
USER_ID="${1:-user-burst-$RANDOM}"

echo "Bursting 25 payments for $USER_ID"
echo

for i in $(seq 1 25); do
  if [ "$i" -le 19 ]; then
    country="US"; device="device-known"
  elif [ $((i % 2)) -eq 0 ]; then
    # Impossible travel: the country flips on every payment, from a device
    # this user has never used.
    country="BD"; device="device-stolen-$i"
  else
    country="US"; device="device-stolen-$i"
  fi

  curl -s -X POST "$API/payments" \
    -H 'content-type: application/json' \
    -d "{\"user_id\":\"$USER_ID\",\"amount\":${i}0.00,\"currency\":\"USD\",\"country\":\"$country\",\"device_id\":\"$device\"}" \
    | python3 "$(dirname "$0")/_show.py" accepted
done

echo
echo "Waiting 4s for the detector and the writer to catch up..."
sleep 4
echo
curl -s "$API/payments?user_id=$USER_ID&limit=25" | python3 "$(dirname "$0")/_show.py" table
echo
echo "User: $USER_ID"
echo "Now run:  make inspect      (offsets + lag)"
