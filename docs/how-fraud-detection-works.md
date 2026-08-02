# How Fraud Detection Works

This document follows a single payment all the way through the system: from the HTTP request,
into PostgreSQL, into Kafka, through the fraud detection service, through the risk engine, back
into Kafka as a decision, and finally back into PostgreSQL where the client can see it.

Every log line, risk score and offset in this document was copied from a real run of the stack.
Nothing here is invented.

The companion documents go deeper on individual pieces:

- [The Fraud Rules](fraud-rules.md) — each rule in detail: SQL, worked examples, limitations
- [Point-in-Time Evaluation](why-at-vs-now.md) — why rules use the payment's timestamp, not `now`

---

## 1. The Cast

Five processes are involved. Each does exactly one job.

```text
┌──────────────┐
│  FastAPI     │  Accepts payments. Writes the row. Publishes the event.
│  (api)       │  NEVER scores anything.
└──────────────┘

┌──────────────┐
│  Kafka       │  Stores events durably in two topics, each with 3 partitions.
│  (broker)    │  Hands them to consumers. Remembers where each group got to.
└──────────────┘

┌──────────────┐
│ PostgreSQL   │  Payment history (what the rules read) and fraud decisions.
└──────────────┘

┌──────────────────┐
│ fraud-detector   │  Consumes payment.events. Runs the rules. Scores.
│ (consumer group) │  Publishes a decision. Writes nothing to the database.
└──────────────────┘

┌──────────────────┐
│ decision-writer  │  Consumes fraud.events. The ONLY writer of the final
│ (consumer group) │  payment status and of the fraud_decisions table.
└──────────────────┘

┌──────────────────┐
│ audit-logger     │  Consumes fraud.events too, in its own group.
│ (consumer group) │  Logs only. Exists to prove fan-out.
└──────────────────┘
```

The important structural fact: **the fraud detector decides, but does not write.** The
decision-writer writes, but does not decide. That separation is what makes the whole pipeline
safe to replay, which section 12 explains.

---

## 2. The Whole Flow at a Glance

```text
  Client
    │
    │ (1) POST /payments
    ▼
  FastAPI ──(2) INSERT payments (status=processing)──► PostgreSQL
    │
    │ (3) publish payment.created, key=user_id
    ▼
  Kafka: payment.events ── partition 2, offset 68
    │
    │ (4) 202 {"status": "processing"} returned to client
    │     ── the HTTP request is now OVER ──
    │
    │ (5) delivered to group "fraud-detector"
    ▼
  Fraud Detector
    │
    ├─(6) SELECT the payment row  ──► PostgreSQL      (fixes the "at" timestamp)
    ├─(7) SELECT user history     ──► PostgreSQL      (what the rules read)
    ├─(8) run 5 rules → list of RuleHit
    ├─(9) Risk Engine: sum → level → decision
    │
    │ (10) publish payment.approved / fraud.detected, key=user_id
    ▼
  Kafka: fraud.events ── partition 2, offset 68
    │
    │ (11) commit offset 69 on payment.events
    │
    ├───────────────────────────┬────────────────────────────┐
    │ (12a) group               │ (12b) group                │
    ▼       "decision-writer"   ▼       "audit-logger"       │
  Decision Writer            Audit Logger                    │
    │                           │                            │
    │ UPDATE payments.status    │ log the decision           │
    │ INSERT fraud_decisions    │ (no writes at all)         │
    ▼                           ▼                            │
  PostgreSQL                  stdout                         │
    │                                                        │
    │ (13) GET /payments/{id}  ◄─────────────────────────────┘
    ▼
  Client sees: {"status": "approved", "risk_score": 0}
```

---

## 3. Step 1 — The Client Makes a Payment

```bash
curl -X POST localhost:8700/payments \
  -H 'content-type: application/json' \
  -d '{
    "user_id":   "doc-trace-1",
    "amount":    50,
    "currency":  "USD",
    "country":   "US",
    "device_id": "device-456"
  }'
```

FastAPI validates the body against `PaymentCreate` (Pydantic). Bad input fails here, before
anything is written or published — a malformed payment never enters the pipeline at all.

---

## 4. Step 2 — The Payment Row Is Written First

```sql
INSERT INTO payments (id, user_id, amount, currency, country, device_id, status, created_at)
VALUES ('payment-270af5f8068b', 'doc-trace-1', 50.00, 'USD', 'US', 'device-456',
        'processing', '2026-08-02T16:28:58Z');
```

The status is `processing`, meaning *"accepted, not yet judged"*.

**Why the row is written before the event is published:** the fraud rules read payment history
from PostgreSQL. If the event were published first, the detector could consume it and query the
history before this payment's own row existed — the frequency rule would then be counting one
payment short of the truth.

This ordering has a cost, covered in section 13.

---

## 5. Step 3 — The Event Is Published to Kafka

```python
event = PaymentCreatedEvent(
    event_id="evt-3f9a1c2b8e77",
    event_type="payment.created",
    timestamp="2026-08-02T16:28:58Z",
    payment_id="payment-270af5f8068b",
    user_id="doc-trace-1",
    amount=50.00,
    currency="USD",
    country="US",
    device_id="device-456",
)

await producer.publish(topic="payment.events", key=event.user_id, event=event)
```

The real log line:

```text
fraud-api | 16:28:58 [info] event.published  event_type=payment.created
                             key=doc-trace-1 topic=payment.events partition=2 offset=68
```

Three things happened in that one call.

**The key chose the partition.** Kafka computed `hash("doc-trace-1") % 3 = 2`. Every event for
this user will always land on partition 2. This is not a detail — it is what makes the rules
correct. All of a user's payments sit on one partition, one consumer owns that partition, so a
user's payments are scored **strictly in order, one at a time**. Key by `payment_id` instead and
two payments from the same user get scored concurrently on different partitions, both read the
same history, and both miss the pattern.

**The broker appended and assigned an offset.** Offset 68 means this record is the 69th ever
written to partition 2 (offsets start at 0). The offset is permanent. Nothing consumes it away.

**The record is now durable.** The producer runs with `acks=all` and `enable_idempotence=True`,
so the broker confirmed the write to its log before returning, and an internal producer retry
cannot silently duplicate the record.

---

## 6. Step 4 — The API Responds, and the Request Is Over

```json
{ "payment_id": "payment-270af5f8068b", "status": "processing" }
```

HTTP 202 Accepted, in roughly 70 ms.

**No fraud analysis has happened yet.** The API cannot tell you the risk score because it does
not know it and has not waited to find out. That is the entire architectural point:

```text
Synchronous design                  This design
──────────────────                  ───────────
Client                              Client
  ↓                                   ↓
API                                 API ──► Kafka
  ↓                                   ↓
Fraud check (slow, and              202 returned immediately
  gets slower as rules grow)          ↓
  ↓                                 Fraud check happens off to the side
Response                              ↓
                                    Client polls for the result later
```

The API's latency no longer depends on how expensive fraud analysis is. In exchange, the client
learns the outcome later. That trade is the whole lesson; everything below is mechanism.

---

## 7. Step 5 — The Fraud Detector Receives the Event

The detector is a separate OS process (`python -m app.consumers.fraud_detector`) running an
infinite poll loop:

```python
batches = await consumer.getmany(timeout_ms=1000)
for _tp, records in batches.items():
    for record in records:
        await handler(record.value)
await consumer.commit()   # only after the work succeeded
```

The real log line:

```text
fraud-detector-1 | 16:29:00 [info] event.received  group=fraud-detector
                                   key=doc-trace-1 topic=payment.events partition=2 offset=68
```

The consumer belongs to group `fraud-detector`. Kafka assigned it partition 2 (along with 0 and
1, since it is currently the only member). If a second detector process starts, Kafka rebalances
and splits the partitions between them.

Note that the detector *pulled* this record. Kafka never pushes. The consumer asks "what is at my
current position on the partitions I own?" and moves forward on its own terms.

---

## 8. Step 6 — Anchoring the Evaluation in Time

Before any rule runs, the detector re-reads the payment row and takes its `created_at`:

```python
payment = await payments_repo.get(session, event.payment_id)
at = payment.created_at if payment is not None else event.timestamp
```

That single variable `at` is passed to every rule, and every rule uses it instead of `now()`.

The reason is subtle and it is the most important correctness property in the system. The detector
runs *behind* the producer. During a burst, by the time payment #15 is being scored, payments
#16 through #25 are already sitting in the database. A rule asking "what is this user's most
recent payment?" would get an answer from the **future** — and the country-change rule would
compare BD against BD and conclude nothing was wrong.

Anchoring to `at` changes the question from *"what does this user's history look like now?"* to
*"what did it look like at the moment this payment happened?"*

It also makes the score **deterministic**: replay the same event tomorrow and rules 1, 2, 4 and 5
produce the identical result. Given at-least-once delivery, replays are guaranteed to happen.

[Point-in-Time Evaluation](why-at-vs-now.md) covers this in depth.

---

## 9. Step 7 — The Rules Run

Each rule is an independent async function with the same shape:

```python
async def some_rule(session, event, at, settings) -> RuleHit | None:
    ...
```

It gets the payment being scored, the moment it happened, and a database session for history.
It returns a `RuleHit(reason, score)` if it fires, or `None` if it does not.

```python
hits: list[RuleHit] = []
for rule in ALL_RULES:
    hit = await rule(session, event, at, settings)
    if hit is not None:
        hits.append(hit)
```

The five rules, in the order they run:

| # | Rule | Question it asks | Score |
|---|---|---|---|
| 1 | Transaction frequency | Did this user make ≥ 20 payments in the 30s up to `at`? | +40 |
| 2 | Large amount | Is this payment over $5,000? | +20 |
| 3 | Repeated blocks | Were ≥ 5 of this user's payments blocked in the 5 min up to `at`? | +20 |
| 4 | Country change | Is the country different from the previous payment, within 1 hour? | +30 |
| 5 | New device | Has this user never paid from this device before `at`? | +10 |

Each rule's SQL, edge cases and limitations are in [The Fraud Rules](fraud-rules.md).

Rules never talk to Kafka, never write to the database, and never know about each other. Adding a
sixth rule means writing one function and adding it to `ALL_RULES`.

For our traced payment — a brand new user's first ever $50 payment from the US — **no rule fires**:
there is no history to be suspicious of, the amount is small, and the first payment from a user
cannot be a "new device" (every device is new then; scoring that would be nonsense).

```text
hits = []
```

---

## 10. Step 8 — The Risk Engine Scores It

The risk engine is pure: hits in, score and decision out. No I/O, no Kafka, no database.

```python
def assess(hits, *, fraud_threshold):
    score = sum(hit.score for hit in hits)
    return RiskAssessment(
        risk_score=score,
        risk_level=classify(score),
        decision="blocked" if score >= fraud_threshold else "approved",
        reasons=[hit.reason for hit in hits],
    )
```

The bands:

```text
  0 ──────── 29 │ 30 ──────── 69 │ 70 ──────── 100 │ 101+
      LOW        │     MEDIUM     │      HIGH       │ CRITICAL
   approved      │    approved    │     BLOCKED     │ BLOCKED
                 │                │
                 │         threshold = 70
```

The maximum possible score is `40 + 20 + 20 + 30 + 10 = 120`, so CRITICAL is only reachable when
every single rule fires at once.

MEDIUM is still approved. It carries its reasons so you can see *why* it was borderline, but one
threshold produces one decision — there is no separate "review" queue in this system.

The real log line for our payment:

```text
fraud-detector-1 | 16:29:00 [info] risk.assessed  payment_id=payment-270af5f8068b
                                   risk_score=0 risk_level=LOW decision=approved reasons=[]
```

---

## 11. Step 9 — The Decision Is Published Back to Kafka

```python
decision_event = FraudDecisionEvent(
    event_id="decision-8b2e91fa4c03",
    event_type="payment.approved",     # or "fraud.detected" when blocked
    payment_id="payment-270af5f8068b",
    user_id="doc-trace-1",
    risk_score=0,
    risk_level="LOW",
    decision="approved",
    reasons=[],
)

await producer.publish("fraud.events", key=event.user_id, event=decision_event)
```

```text
fraud-detector-1 | 16:29:00 [info] event.published  event_type=payment.approved
                                   key=doc-trace-1 topic=fraud.events partition=2 offset=68
```

Note the `event_type` mapping — one topic carries two kinds of event:

```text
decision == "blocked"   →  event_type = "fraud.detected"
decision == "approved"  →  event_type = "payment.approved"
```

Consumers of `fraud.events` must therefore branch on `event_type` rather than assume.

The key is `user_id` again, so a user's decisions stay ordered relative to each other on the
downstream topic too.

---

## 12. Step 10 — The Offset Is Committed

Only now, after the decision has been successfully published, does the detector commit:

```text
fraud-detector-1 | 16:29:00 [info] offsets.committed
                                   group=fraud-detector partitions={'payment.events-2': 69}
```

Committed offset 69 means *"the next record I want is 69"* — that is, everything up to and
including offset 68 is done. The value is stored by the broker in the internal
`__consumer_offsets` topic, against this group id.

**Commit after the work, never before.** Auto-commit (the default) commits on a timer regardless
of whether the handler succeeded, which means a crash can skip an event permanently. Committing
afterwards gives **at-least-once** delivery: if the detector dies mid-batch, the uncommitted
records are delivered again on restart.

At-least-once means duplicates are possible, which is why the next step must be idempotent.

---

## 13. Step 11 — Two Groups Consume the Same Decision

`fraud.events` has two subscribers, in **different consumer groups**. Both receive every single
record, and each tracks its own offsets independently.

### decision-writer — the only writer

```text
fraud-decision-writer | 16:29:00 [info] event.received  group=decision-writer
                                        topic=fraud.events partition=2 offset=68
fraud-decision-writer | 16:29:00 [info] payment.status.updated
                                        payment_id=payment-270af5f8068b
                                        status=approved risk_score=0 first_time=True
```

It does two writes:

```sql
INSERT INTO fraud_decisions (id, payment_id, user_id, risk_score, risk_level, decision, reasons)
VALUES (...)
ON CONFLICT (payment_id) DO NOTHING;      -- the idempotency guard

UPDATE payments SET status = 'approved' WHERE id = 'payment-270af5f8068b';
```

That `ON CONFLICT DO NOTHING` is what makes at-least-once delivery safe. `fraud_decisions.payment_id`
carries a UNIQUE constraint, so a redelivered event inserts nothing and the status update simply
writes the same value again. Processing an event twice produces exactly the same database state as
processing it once. PostgreSQL is the deduplication store — no Redis required.

`first_time=True` in the log means this was the first application of this decision.
`first_time=False` means the unique constraint absorbed a duplicate, which is the normal and
correct outcome of a replay.

### audit-logger — the fan-out proof

```text
fraud-audit-logger | 16:29:00 [info] event.received  group=audit-logger
                                     topic=fraud.events partition=2 offset=68
fraud-audit-logger | 16:29:00 [info] audit.decision  event_type=payment.approved
                                     payment_id=payment-270af5f8068b risk_score=0 reasons=[]
```

Same topic. Same partition. Same offset. **Different group** — so it got its own copy.

This is the difference between Kafka and a work queue. In a queue, whichever consumer grabbed the
message first would be the only one to see it. Here, a new group can be added at any time and it
will receive everything, including history, without affecting anybody else.

---

## 14. Step 12 — The Client Learns the Outcome

```bash
curl localhost:8700/payments/payment-270af5f8068b
```

```json
{
  "payment_id": "payment-270af5f8068b",
  "user_id": "doc-trace-1",
  "amount": "50.00",
  "country": "US",
  "device_id": "device-456",
  "status": "approved",
  "created_at": "2026-08-02T16:28:58Z",
  "risk_score": 0,
  "risk_level": "LOW"
}
```

And the reasoning behind it:

```bash
curl localhost:8700/payments/payment-270af5f8068b/fraud
```

```json
{
  "payment_id": "payment-270af5f8068b",
  "risk_score": 0,
  "risk_level": "LOW",
  "decision": "approved",
  "reasons": [],
  "created_at": "2026-08-02T16:29:00Z"
}
```

Before the pipeline settles, `/fraud` returns 404 (`"no decision yet - the payment is still being
analysed"`) and `risk_score` is `null` rather than `0`. The API says *"I don't know yet"* instead
of guessing — a payment in flight and a payment scored zero are genuinely different states.

Measured settle time on this stack: **around 40 ms** from POST to final status.

---

## 15. The Complete Trace, in One Place

Every line below is real output from a single payment, in the order it happened:

```text
fraud-api             | 16:28:58 event.published        event_type=payment.created  key=doc-trace-1  topic=payment.events  partition=2 offset=68
fraud-detector-1      | 16:29:00 event.received         event_type=payment.created  key=doc-trace-1  topic=payment.events  partition=2 offset=68  group=fraud-detector
fraud-detector-1      | 16:29:00 risk.assessed          payment_id=payment-270af5f8068b  risk_score=0  risk_level=LOW  decision=approved  reasons=[]
fraud-detector-1      | 16:29:00 event.published        event_type=payment.approved key=doc-trace-1  topic=fraud.events    partition=2 offset=68
fraud-detector-1      | 16:29:00 offsets.committed      group=fraud-detector  partitions={'payment.events-2': 69}
fraud-decision-writer | 16:29:00 event.received         event_type=payment.approved key=doc-trace-1  topic=fraud.events    partition=2 offset=68  group=decision-writer
fraud-decision-writer | 16:29:00 payment.status.updated payment_id=payment-270af5f8068b  status=approved  risk_score=0  first_time=True
fraud-audit-logger    | 16:29:00 event.received         event_type=payment.approved key=doc-trace-1  topic=fraud.events    partition=2 offset=68  group=audit-logger
fraud-audit-logger    | 16:29:00 audit.decision         payment_id=payment-270af5f8068b  risk_level=LOW  risk_score=0  reasons=[]
```

Read the `topic / partition / offset` triple on every line. That triple is the address of a record
in the log, and following it across services is the fastest way to build an intuition for how
Kafka actually delivers work.

---

## 16. A Worked Example of Each Rule

Each rule triggered in isolation, with a brand new user so exactly one could fire. Real results:

| Scenario | Score | Level | Decision | Reasons |
|---|---|---|---|---|
| One $7,500 payment, no history | 20 | LOW | approved | `large_amount` |
| Second payment from a different device | 10 | LOW | approved | `new_device` |
| Second payment from a different country, same device | 30 | MEDIUM | approved | `country_change` |
| Different country **and** different device | 40 | MEDIUM | approved | `country_change`, `new_device` |
| 20 payments in 30s, same country and device | 40 | MEDIUM | approved | `high_transaction_frequency` |

Every one of those is **approved**. Now the takeover from `make burst` — velocity, a country hop
from a new device, and rule 3's feedback once five payments have already been blocked:

```json
{ "payment_id": "payment-55a4005c258c", "risk_score": 100, "risk_level": "HIGH",
  "decision": "blocked",
  "reasons": ["high_transaction_frequency", "repeated_blocked_payments",
              "country_change", "new_device"] }
```

That contrast is the design in one table: **no single rule blocks a payment.** The heaviest is
worth 40 against a threshold of 70. Fraud here is always a *combination* of signals — which is
exactly why a scoring model exists instead of a chain of if-statements.

The SQL, edge cases, regressions and limitations for each rule: [The Fraud Rules](fraud-rules.md).

---

## 17. What Happens When Something Breaks

The value of the pipeline shape shows up when a piece fails.

**The fraud detector is down.** The API keeps accepting payments and publishing events. Nothing is
lost — the records sit in `payment.events`, and consumer lag climbs:

```bash
docker compose stop fraud-detector && make burst && make inspect
```

Every payment stays at `processing`. Start the detector again and it resumes from its committed
offset and drains the backlog to zero. Verified: lag climbed to exactly 15, then returned to 0.

**The decision-writer is down.** The detector still scores and still publishes. `fraud.events`
happily accepts records that nobody is currently reading — Kafka does not care whether a consumer
exists. Payments stay at `processing` until the writer returns and catches up.

**The detector crashes mid-batch.** The offsets for that batch were never committed, so those
records are redelivered on restart. Duplicated work, no lost work. The unique constraint on
`fraud_decisions.payment_id` absorbs the duplicate write.

**A handler raises.** The exception is logged with its `topic/partition/offset` and re-raised, so
the offset is *not* committed. This system has no retry topic or dead-letter queue yet, so a
permanently failing record would block its partition — a real production system would route it to
`fraud.retry` and then `fraud.dlq` after N attempts.

**The API dies between the INSERT and the publish.** This is the one genuine gap. The two steps
are not atomic (a *dual write*), so the payment row is stranded at `processing` with no event to
ever score it. The correct fix is a transactional outbox, which is out of scope here — so instead
the gap is made **visible**:

```bash
curl "localhost:8700/payments/stuck?older_than_seconds=60"
```

Hiding a known limitation is worse than exposing it.

---

## 18. Replaying the Whole Thing

Because consuming never deletes anything, any group can be rewound:

```bash
./scripts/replay.sh decision-writer fraud.events
```

This stops the consumer (a group's offsets can only be reset while it has no active members),
resets it to the earliest offset, and restarts it. Every decision ever made is re-delivered and
re-applied.

Verified result: **138 decisions before the replay, 138 after**, with every log line reading
`first_time=False`. The database is byte-identical because the writes are idempotent.

That is the practical payoff of the whole design — decide in one service, write in another, key by
`user_id`, commit after the work, and guard the write with a unique constraint.

---

## 19. Where Each Step Lives in the Code

| Step | File |
|---|---|
| Accept payment, publish event | [`app/api/payments.py`](../backend/app/api/payments.py) |
| Producer, keys, `acks=all` | [`app/kafka/producer.py`](../backend/app/kafka/producer.py) |
| Event schemas (the contract) | [`app/schemas/events.py`](../backend/app/schemas/events.py) |
| Consumer loop, groups, commits | [`app/kafka/consumer.py`](../backend/app/kafka/consumer.py) |
| Detector entry point | [`app/consumers/fraud_detector.py`](../backend/app/consumers/fraud_detector.py) |
| Anchoring `at`, running rules | [`app/fraud/detector.py`](../backend/app/fraud/detector.py) |
| The five rules | [`app/fraud/rules.py`](../backend/app/fraud/rules.py) |
| Scoring, bands, threshold | [`app/fraud/risk_engine.py`](../backend/app/fraud/risk_engine.py) |
| History queries the rules use | [`app/repositories/payments.py`](../backend/app/repositories/payments.py) |
| Idempotent decision write | [`app/repositories/fraud_decisions.py`](../backend/app/repositories/fraud_decisions.py) |
| Status writer | [`app/consumers/decision_writer.py`](../backend/app/consumers/decision_writer.py) |
| Fan-out proof | [`app/consumers/audit_logger.py`](../backend/app/consumers/audit_logger.py) |
| Offsets, ownership, lag | [`app/kafka/inspect.py`](../backend/app/kafka/inspect.py) |

---

## 20. Watch It Yourself

```bash
make up
```

```bash
make demo
```

```bash
make detector-logs
```

Then submit a payment and read the log lines as they appear. The `topic / partition / offset`
triple on every line is the whole of Kafka, made concrete.
