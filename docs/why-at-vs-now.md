# Point-in-Time Evaluation in Real-Time Fraud Detection

This document explains why a fraud detection system should evaluate a payment using the payment's own timestamp rather than the current time (`now`).

The idea is simple:

> When scoring an event, don't ask what the world looks like now. Ask what the world looked like when that event happened.

This is especially important in a Kafka-based fraud detection system because the fraud detector is asynchronous, events can arrive late or be replayed, and newer transactions may already exist in the database when an older transaction is scored.

---

## 1. The Fraud Detection Architecture

A simplified architecture looks like this:

```text
Payment Service
      ↓
Kafka: payment.events
      ↓
Fraud Detection Service
      ↓
Risk Engine
      ↓
fraud.detected
      ↓
Payment Service
```

The Payment Service produces payment events.

Kafka stores and delivers those events.

The Fraud Detection Service consumes the events and evaluates them.

The Risk Engine applies fraud rules and calculates a risk score.

If the payment is suspicious, the system can publish a `fraud.detected` event.

---

## 2. The Problem

Suppose a user makes 20 payments very quickly:

```text
Payment #1  → 10:00:01
Payment #2  → 10:00:02
Payment #3  → 10:00:03
...
Payment #15 → 10:00:15
Payment #16 → 10:00:16
...
Payment #20 → 10:00:20
```

Now suppose the fraud detector is processing payment #15 at:

```text
10:00:25
```

Because the payment producer and fraud detector are asynchronous, payments #16 through #20 may already have been inserted into the database.

So the database might look like:

```text
#15  → 10:00:15
#16  → 10:00:16
#17  → 10:00:17
#18  → 10:00:18
#19  → 10:00:19
#20  → 10:00:20
```

The fraud detector is supposed to answer:

> "What did this user's history look like when payment #15 happened?"

But a naive implementation may accidentally ask:

> "What is this user's most recent payment right now?"

Those are two different questions.

---

# 3. Why Using `now` Is Wrong

Imagine a fraud rule needs to find the user's most recent payment.

A naive query could be:

```sql
SELECT *
FROM payments
WHERE user_id = :user_id
ORDER BY created_at DESC
LIMIT 1;
```

This query answers:

> "What is the user's most recent payment right now?"

At `10:00:25`, the query could return:

```text
Payment #20
```

But the fraud detector is currently scoring:

```text
Payment #15
```

The detector has accidentally allowed future information to influence the decision.

The correct question is:

> "What was the user's history at the time payment #15 occurred?"

---

# 4. Country-Change Example

Consider this transaction history:

```text
Payment #14 → Bangladesh
Payment #15 → India
Payment #16 → Bangladesh
Payment #17 → Bangladesh
Payment #18 → Bangladesh
Payment #19 → Bangladesh
Payment #20 → Bangladesh
```

When payment #15 happened, the relevant history was:

```text
Previous country → Bangladesh
Current country  → India
```

So the detector should identify:

```text
Bangladesh → India
```

as a potentially suspicious geographic change.

But if the detector asks:

```text
"What is the user's most recent payment?"
```

it could get payment #20:

```text
Payment #20 → Bangladesh
```

Now the detector is looking at information that happened after payment #15.

The important point is not simply that the countries are different.

The important point is that the fraud detector must evaluate the state of the system **at the moment payment #15 occurred**.

---

# 5. The `at` Boundary

Instead of:

```python
get_user_history(user_id)
```

use an explicit time boundary:

```python
get_user_history(
    user_id=user_id,
    at=payment.created_at,
)
```

The `at` parameter means:

> "Evaluate the user's history as it existed at this point in time."

For example:

```sql
SELECT *
FROM payments
WHERE user_id = :user_id
  AND created_at < :payment_created_at
ORDER BY created_at DESC;
```

If we are scoring payment #15:

```text
payment_created_at = 10:00:15
```

then the query can use:

```text
#1
#2
#3
...
#14
```

but not:

```text
#16
#17
#18
...
#20
```

because those transactions happened later.

---

# 6. Think of It as "Time Travel"

You can think of the fraud detector as asking the database:

> "Pretend we are standing at `10:00:15`. What did we know about this user at that exact moment?"

For example:

```text
             Timeline

#14       #15       #16       #17       #18
 |---------|---------|---------|---------|
BD         IN        BD        BD        BD
           ↑
           |
       Score this

           at = #15.created_at
```

The detector should evaluate payment #15 using only information available up to that point.

This concept is commonly called:

- Point-in-time evaluation
- As-of querying
- Temporal querying
- Time-aware feature computation

---

# 7. Why This Makes the Score Deterministic

Suppose payment #15 is processed today:

```text
Payment #15
     ↓
Fraud Detector
     ↓
Score = 85
```

Tomorrow, the same payment is replayed.

If the fraud detector uses the current database state:

```text
"What is in the database right now?"
```

the database could contain thousands of newer payments.

The result could become:

```text
Today:
Payment #15 → Score = 85

Tomorrow:
Payment #15 → Score = 32
```

That is a problem.

The exact same event should not produce a different fraud decision simply because it was replayed later.

Instead, the detector should always evaluate the payment against the historical state represented by its own timestamp:

```text
Payment #15
     ↓
at = payment_15.created_at
     ↓
Evaluate historical state
     ↓
Score = 85
```

Then:

```text
Replay today:
Payment #15 → Score = 85

Replay tomorrow:
Payment #15 → Score = 85
```

This is deterministic processing.

---

# 8. Why This Matters with Kafka

Kafka consumers commonly use **at-least-once delivery**.

At-least-once delivery means the same event can sometimes be delivered more than once.

For example:

```text
Kafka
  ↓
Payment #15
  ↓
Fraud Detection Service
  ↓
Process successfully
  ↓
Database/network issue before offset commit
  ↓
Kafka retries
  ↓
Payment #15 AGAIN
```

The same payment can therefore be processed multiple times.

We want:

```text
Payment #15
     ↓
Score = 85

Payment #15 replayed
     ↓
Score = 85
```

Not:

```text
Payment #15
     ↓
Score = 85

Payment #15 replayed later
     ↓
Score = 32
```

Using the payment's own timestamp as the evaluation boundary makes replay behavior much more predictable.

---

# 9. Why Replays Happen

At-least-once delivery is common because consumers generally need to avoid losing events.

A simplified processing flow might look like:

```text
1. Kafka delivers payment #15
          ↓
2. Consumer processes payment #15
          ↓
3. Fraud score is calculated
          ↓
4. Database operation happens
          ↓
5. Consumer commits Kafka offset
```

Imagine something fails between steps 4 and 5:

```text
Kafka delivers payment #15
          ↓
Consumer processes payment #15
          ↓
Fraud score calculated
          ↓
Database operation succeeds
          ↓
Consumer crashes
          X
Offset was not committed
```

Kafka may deliver payment #15 again.

Therefore, replay is not an unusual edge case. It is a normal consideration when designing event-driven systems.

---

# 10. The Key Principle

When scoring an event:

> **Don't ask what the world looks like now. Ask what the world looked like when that event happened.**

Instead of:

```python
score(payment, now())
```

use:

```python
score(
    payment,
    at=payment.created_at,
)
```

The payment's timestamp becomes the temporal boundary for calculating its fraud features.

---

# 11. Every Fraud Feature Should Respect the Same Boundary

Suppose we have these fraud rules:

```text
Payment #15
     │
     ├── Transaction frequency
     │       └── Payments before #15
     │
     ├── Amount anomaly
     │       └── Transactions before #15
     │
     ├── Geographic change
     │       └── Previous country before #15
     │
     ├── Failed transactions
     │       └── Failures before #15
     │
     └── Device change
             └── Devices seen before #15
```

Each rule should use the same point-in-time boundary.

For example:

### Transaction Frequency

Ask:

> How many transactions did this user make before this payment?

Not:

> How many transactions does this user have now?

### Amount Anomaly

Ask:

> Was this payment unusually large compared with the user's previous behavior?

Not:

> Is this payment large compared with all transactions that exist now?

### Geographic Change

Ask:

> What was the user's previous location before this payment?

Not:

> What is the user's latest location?

### Failed Transactions

Ask:

> How many failed transactions existed before this payment?

Not:

> How many failed transactions exist today?

### Device Change

Ask:

> Which devices had this user used before this payment?

Not:

> Which devices has this user ever used?

---

# 12. Event Time vs Processing Time

This distinction is important in streaming systems.

There are two different timestamps:

```text
Event Time
    ↓
When the payment actually happened

Processing Time
    ↓
When the fraud detector processed the event
```

For example:

```text
Payment happened:
10:00:15

Fraud detector processed it:
10:00:25
```

So:

```text
event_time    = 10:00:15
processing_time = 10:00:25
```

For historical fraud features, we usually want the event's temporal context:

```text
at = event_time
```

not:

```text
at = processing_time
```

The processing service may be delayed, but that delay should not change what was true when the payment happened.

---

# 13. A Simple Mental Model

Think about every payment as creating a snapshot of the user's world.

For payment #15:

```text
                    Payment #15
                         │
                         ▼
              ┌────────────────────┐
              │  Historical State  │
              │                    │
              │ Payments before #15│
              │ Previous country   │
              │ Previous devices   │
              │ Failed payments    │
              │ Historical amounts │
              └────────────────────┘
                         │
                         ▼
                    Fraud Rules
                         │
                         ▼
                    Risk Score
```

The detector should not use information from the future:

```text
Payment #15
     │
     ├── Historical information
     │       ├── #1
     │       ├── #2
     │       ├── ...
     │       └── #14
     │
     └── Future information
             ├── #16
             ├── #17
             ├── ...
             └── #20

             DO NOT USE
```

---

# 14. A More Concrete Python Example

A simple conceptual implementation could look like:

```python
def score_payment(payment):
    history = get_user_history(
        user_id=payment.user_id,
        at=payment.created_at,
    )

    transaction_frequency = calculate_transaction_frequency(
        history=history,
    )

    amount_anomaly = calculate_amount_anomaly(
        payment=payment,
        history=history,
    )

    country_change = detect_country_change(
        payment=payment,
        history=history,
    )

    failed_transactions = count_failed_transactions(
        history=history,
    )

    device_change = detect_device_change(
        payment=payment,
        history=history,
    )

    return calculate_risk_score(
        transaction_frequency=transaction_frequency,
        amount_anomaly=amount_anomaly,
        country_change=country_change,
        failed_transactions=failed_transactions,
        device_change=device_change,
    )
```

The important part is:

```python
at=payment.created_at
```

That one boundary should propagate into the feature calculations.

---

# 15. Database Query Example

A historical lookup might look like:

```sql
SELECT *
FROM payments
WHERE user_id = :user_id
  AND created_at < :payment_created_at
ORDER BY created_at DESC;
```

If payment #15 is:

```text
created_at = 10:00:15
```

then the query only considers payments before:

```text
10:00:15
```

The detector therefore gets:

```text
#14
#13
#12
...
```

and not:

```text
#16
#17
#18
...
#20
```

This prevents future information from leaking into the fraud decision.

---

# 16. The Bigger Distributed-Systems Principle

This is not only a fraud-detection concept.

It is a general distributed-systems principle:

> **Separate event time from processing time.**

In asynchronous systems:

```text
Event happens
     ↓
Event enters Kafka
     ↓
Event waits in queue
     ↓
Consumer receives event
     ↓
Consumer processes event
```

There can be a delay between:

```text
when something happened
```

and:

```text
when the system processed it
```

Therefore, business logic that depends on historical state should usually be anchored to the event's logical time.

---

# 17. Summary

The `at=payment.created_at` boundary provides three important properties.

## 1. No Future Data Leakage

The fraud detector cannot accidentally use payments that happened after the payment being scored.

```text
Payment #15
     ↓
Use #1 through #14
     ↓
Ignore #16 through #20
```

## 2. Correct Historical Evaluation

The detector evaluates the user's state as it existed when the payment actually happened.

```text
Event Time
     ↓
Historical State
     ↓
Fraud Features
     ↓
Risk Score
```

## 3. Deterministic Replay

Replaying the same Kafka event later should produce the same fraud score.

```text
Payment #15
     ↓
Score = 85

Replay Payment #15
     ↓
Score = 85
```

---

# Final Takeaway

The core idea can be summarized in one sentence:

> **When scoring an event, don't ask what the world looks like now. Ask what the world looked like when that event happened.**

For a Kafka-based real-time fraud detection system:

```text
Payment Event
     ↓
event_time = payment.created_at
     ↓
Point-in-Time Historical State
     ↓
Fraud Features
     ↓
Risk Rules
     ↓
Risk Score
     ↓
fraud.detected
```

This design makes the system more correct, replayable, testable, and deterministic, especially when Kafka delivers events at least once and processing can happen after newer events have already reached the database.
