# The Fraud Rules

Five rules, each an independent function. This document covers how they fit together, then each
rule in detail: configuration, SQL, worked examples that fire and that don't, and limitations.

Every score and JSON response below was captured from a real run — each rule was triggered in
isolation with a brand new user so that exactly one rule could fire.

For how an event reaches the rules in the first place, see
[How Fraud Detection Works](how-fraud-detection-works.md).

---

## 1. Overview

| # | Rule | Fires when | Score |
|---|---|---|---|
| 1 | [Transaction frequency](#6-rule-1--transaction-frequency) | ≥ 20 payments by this user in the 30s up to `at` | +40 |
| 2 | [Large amount](#7-rule-2--large-amount) | amount > $5,000 | +20 |
| 3 | [Repeated blocked payments](#8-rule-3--repeated-blocked-payments) | ≥ 5 of this user's payments blocked in the 5 min up to `at` | +20 |
| 4 | [Country change](#9-rule-4--country-change) | country differs from the previous payment, within 1 hour | +30 |
| 5 | [New device](#10-rule-5--new-device) | this user has never paid from this device before `at` | +10 |

Every threshold lives in [`app/core/config.py`](../backend/app/core/config.py) and is overridable
by environment variable — see [`.env.example`](../.env.example).

---

## 2. Every Rule Has the Same Shape

```python
async def some_rule(
    session: AsyncSession,          # for reading history
    event: PaymentCreatedEvent,     # the payment being scored
    at: datetime,                   # WHEN it happened - never now()
    s: Settings,                    # thresholds
) -> RuleHit | None:                # a hit, or nothing
    ...
```

```python
@dataclass(frozen=True)
class RuleHit:
    reason: str   # e.g. "country_change" - ends up in the decision event
    score: int    # e.g. 30
```

The consequences of this shape are worth naming:

- **Rules cannot see each other.** No rule knows what any other decided. No ordering dependency, no
  shared state — they can be read, tested and changed one at a time.
- **Rules never write.** No rule touches Kafka or mutates the database. They read history and
  return a value.
- **Rules never decide.** A rule cannot block a payment. It contributes a score; the risk engine
  decides.
- **Adding a rule is a three-line change:** write the function, add it to `ALL_RULES`, add its
  thresholds to `Settings`.

```python
ALL_RULES = (
    transaction_frequency,
    large_amount,
    repeated_blocks,
    country_change,
    new_device,
)
```

Rule 2 uses neither `session` nor `at`, and still takes both. Uniformity is worth more than saving
two parameters: the detector loops over `ALL_RULES` without knowing what any rule needs, and a rule
can start needing history later without changing any calling code.

---

## 3. The `at` Parameter Is the Whole Ballgame

Every rule that reads history is anchored to `at` — the `created_at` of the payment being scored —
and never to `now()`.

The fraud detector runs *behind* the producer. During a burst, when payment #15 is being scored,
payments #16–#25 are already in the database. A rule asking *"what is this user's most recent
payment?"* gets an answer from the future.

That is not hypothetical. It is a bug this codebase shipped with, and it silenced two rules
completely — details in each rule's regression section below.

Anchoring to `at` also makes rules 1, 2, 4 and 5 **replay-deterministic**: re-score the same event
tomorrow and you get the identical answer. With at-least-once delivery, replays are guaranteed.
Rule 3 is the documented exception.

Full treatment: [Point-in-Time Evaluation](why-at-vs-now.md).

---

## 4. How Scores Become Decisions

```python
score = sum(hit.score for hit in hits)
```

```text
  0 ──────── 29 │ 30 ──────── 69 │ 70 ──────── 100 │ 101+
      LOW        │     MEDIUM     │      HIGH       │ CRITICAL
   approved      │    approved    │     BLOCKED     │ BLOCKED
                 │                │
                 │        threshold = 70
```

Maximum possible score: `40 + 20 + 20 + 30 + 10 = 120`. CRITICAL therefore requires **all five**
rules firing at once.

MEDIUM is still approved. It carries its reasons so the near-miss is visible, but one threshold
produces one decision — there is no manual review queue in this system.

---

## 5. No Single Rule Can Block a Payment

The highest-scoring rule is worth 40. The threshold is 70. This is deliberate.

```text
frequency alone        40  → MEDIUM, approved
country change alone   30  → MEDIUM, approved
large amount alone     20  → LOW,    approved
new device alone       10  → LOW,    approved

frequency + country + device   80  → HIGH, BLOCKED
```

A user making many small payments quickly is not fraud. A user travelling is not fraud. A user
buying a new phone is not fraud. **All three at once**, from an account that has never behaved that
way, is fraud.

That is the argument for a scoring model rather than a chain of if-statements: weak signals combine
into a strong one, without any single weak signal causing a false positive.

---

## 6. Rule 1 — Transaction Frequency

**+40 points** — the heaviest rule in the system.

> Did this user make 20 or more payments in the 30 seconds leading up to this one?

### What it detects

Automation. A human does not make twenty payments in half a minute; a script testing stolen card
details does. This is the classic *card testing* pattern — an attacker with a list of stolen
credentials fires small payments in rapid succession to find which ones still work.

It scores highest because velocity is the hardest signal to fake innocently. A user can legitimately
travel, buy something expensive, or replace their phone. Twenty payments in thirty seconds has
almost no innocent explanation.

### Configuration

```python
rule_frequency_count: int = 20            # RULE_FREQUENCY_COUNT
rule_frequency_window_seconds: int = 30   # RULE_FREQUENCY_WINDOW_SECONDS
rule_frequency_score: int = 40            # RULE_FREQUENCY_SCORE
```

### The code

```python
async def transaction_frequency(session, event, at, s) -> RuleHit | None:
    count = await payments_repo.count_in_window(
        session, event.user_id, seconds=s.rule_frequency_window_seconds, at=at
    )
    if count >= s.rule_frequency_count:
        return RuleHit("high_transaction_frequency", s.rule_frequency_score)
    return None
```

```sql
SELECT count(*)
FROM payments
WHERE user_id = :user_id
  AND created_at >= :at - interval '30 seconds'
  AND created_at <= :at;               -- the upper bound is the important half
```

**The count includes the payment being scored.** The API writes the row before publishing the
event, so the payment is already in the table when this query runs. That is why reaching the
threshold takes exactly 20 payments, not 21.

### It fires

Twenty payments for a new user inside 30 seconds, same country and device throughout:

```json
{
  "payment_id": "payment-f70a549b8fd4",
  "risk_score": 40,
  "risk_level": "MEDIUM",
  "decision": "approved",
  "reasons": ["high_transaction_frequency"]
}
```

**MEDIUM, and still approved.** High velocity alone is suspicious, not conclusive — a user
refreshing a checkout page in frustration produces a burst too.

### It does not fire

The same twenty payments spread over an hour. Each payment's own 30-second window contains only
itself, so `count = 1`. Volume is not the signal; **concentration in time** is.

### The window slides per payment

It is not a fixed clock bucket — it is anchored to each payment's own timestamp:

```text
                    ┌──── window for #20 ────┐
    #1  #2  ...  #14 #15 #16 #17 #18 #19 #20
    │                │                      │
 10:00:00         10:00:15               10:00:29

  window for #15: 09:59:45 → 10:00:15   contains 15 payments  →  no hit
  window for #20: 09:59:59 → 10:00:29   contains 20 payments  →  HIT
```

In a burst, the early payments genuinely were not suspicious yet, and the rule correctly says so.

### Regression — why the upper bound exists

Without `created_at <= at`, the query means *"payments in the last 30 seconds from now"*, and `now`
is whenever the detector happens to run:

```text
Payments #1-#30 all written by 10:00:30. Detector scores #5 at 10:00:31.

Without the upper bound: "between 10:00:01 and now"   →  count = 30  →  HIT
With it (at = #5's created_at): "09:59:35 → 10:00:05" →  count = 5   →  no hit
```

The first answer is wrong: payment #5 was the fifth payment, and payments #6–#30 existing later
cannot retroactively make it suspicious. It also destroys determinism, so a replay would rewrite
history.

### Limitations

- **Per-user only.** An attacker spreading twenty payments across twenty stolen accounts triggers
  nothing. That needs a device- or IP-level aggregate, which this system does not model.
- **Absolute, not relative.** A merchant legitimately doing 20 payments a second is flagged
  constantly. A production rule would compare against a per-user baseline.
- **It queries the payments table on every event.** Fine here thanks to the `(user_id, created_at)`
  index; at real volume you would keep a windowed counter in the stream processor's own state —
  precisely what Kafka Streams state stores exist for.

---

## 7. Rule 2 — Large Amount

**+20 points** — the only rule that needs no history at all.

> Is this single payment worth more than $5,000?

### What it detects

The cash-out. An attacker who has compromised an account eventually has to extract value, and that
final payment is usually far larger than anything the account normally does.

### Configuration

```python
rule_large_amount: float = 5000.0      # RULE_LARGE_AMOUNT
rule_large_amount_score: int = 20      # RULE_LARGE_AMOUNT_SCORE
```

### The code

```python
async def large_amount(session, event, at, s) -> RuleHit | None:
    if float(event.amount) > s.rule_large_amount:
        return RuleHit("large_amount", s.rule_large_amount_score)
    return None
```

That is the entire rule — no query, no `session`, no `at`. The comparison is strictly greater:

```text
amount = 4999.99   →  no hit
amount = 5000.00   →  no hit      ← exactly at the limit is fine
amount = 5000.01   →  HIT, +20
```

### It fires

A brand new user, one $7,500 payment, no history whatsoever:

```json
{
  "payment_id": "payment-a8c3cbdd946a",
  "risk_score": 20,
  "risk_level": "LOW",
  "decision": "approved",
  "reasons": ["large_amount"]
}
```

**LOW, approved.** Plenty of people buy expensive things. Note this fired on the user's *first ever*
payment — no other rule can do that, since the rest all need history.

### Where it matters

Twenty points rarely decides anything alone, but it is often what pushes a borderline payment up:

```text
country change 30 + new device 10                      = 40   MEDIUM, approved
country change 30 + new device 10 + large amount 20    = 60   MEDIUM, approved
… + frequency 40                                       = 100  HIGH,   BLOCKED
```

A payment from a new device in a new country is a MEDIUM. The same payment for $9,000 is a more
worrying MEDIUM, and the score records that difference even when the decision does not change.

### Limitations

This is the weakest of the five, deliberately.

- **A global constant, not a per-user baseline.** $5,000 is enormous for one user and routine for
  another. The genuinely useful version asks *"is this far outside this user's normal range?"* —
  which needs history, making it look like the other four rules.
- **Currency is ignored.** `currency` is stored and carried through the event, but the comparison
  treats every amount as USD. 5,000 JPY (about $32) trips the rule. Fixing it needs an FX source,
  beyond this project's scope — but it is a real bug in any system that ships this rule as written.
- **Trivially evadable.** An attacker who knows the limit splits the cash-out into two payments of
  $2,600. That evasion produces two payments in quick succession — which is what rule 1 is for. The
  rules cover each other's gaps.

---

## 8. Rule 3 — Repeated Blocked Payments

**+20 points** — the only rule that reads the system's own past decisions.

> Have 5 or more of this user's payments already been blocked in the last 5 minutes?

### What it detects

Persistence after refusal. A legitimate user whose payment is declined stops, or contacts support.
An attacker keeps going, because they are working through a list.

This rule creates a **feedback loop**: fraud decisions become inputs to future fraud decisions. Once
an account has been repeatedly blocked, everything it does afterwards is scored more harshly.

### A deliberate deviation from the PRD

The specification called this *"multiple failed transactions"* — five **failed** payments in five
minutes. That rule could never have fired. The statuses in this system are:

```text
processing  →  approved
            →  blocked
```

Nothing ever produces `failed`; there is no payment gateway, so there is nothing to fail. The rule
as specified would have been dead code that looked alive — arguably worse than no rule, because it
creates a false sense of coverage.

`blocked` is the equivalent signal and is strictly more useful, because it closes the feedback loop
above.

### Configuration

```python
rule_blocked_count: int = 5              # RULE_BLOCKED_COUNT
rule_blocked_window_seconds: int = 300   # RULE_BLOCKED_WINDOW_SECONDS  (5 minutes)
rule_blocked_score: int = 20             # RULE_BLOCKED_SCORE
```

The window is ten times longer than the frequency rule's. Being blocked is far rarer and more
serious than making a payment, so the signal is worth remembering for longer.

### The code

```python
async def repeated_blocks(session, event, at, s) -> RuleHit | None:
    count = await payments_repo.count_blocked_in_window(
        session, event.user_id, seconds=s.rule_blocked_window_seconds, at=at
    )
    if count >= s.rule_blocked_count:
        return RuleHit("repeated_blocked_payments", s.rule_blocked_score)
    return None
```

```sql
SELECT count(*)
FROM payments
WHERE user_id = :user_id
  AND status = 'blocked'
  AND created_at >= :at - interval '5 minutes'
  AND created_at <= :at;
```

### The honest caveat — not replay-deterministic

Every other rule reads facts fixed at the moment the payment was created: amount, country, device,
timestamp. Replay a year later and those are unchanged.

This rule reads `status`, written **asynchronously by a different consumer** (`decision-writer`)
milliseconds after the detector scores the payment:

```text
t=0ms    payment #21 scored → blocked → published to fraud.events
t=+3ms   payment #22 scored → reads status of #21 → still 'processing'!
t=+8ms   decision-writer applies #21's decision → status becomes 'blocked'
```

So the count depends on how far the writer has caught up. The same event scored twice can produce
different results, and a replay may score higher than the original run.

**Accepted, not overlooked:**

1. Five blocked payments in five minutes is a slow-moving signal. Missing one because a status
   landed 4 ms late does not change the picture — the next payment catches it.
2. The alternative is the detector writing status itself, collapsing the separation between
   *deciding* and *writing* and making replays destructive.
3. It is worth 20 points, not 70. It can never single-handedly cause a wrong decision.

### It fires

Only late in a takeover, once five payments have actually been blocked. From a real `make burst`:

```json
{
  "payment_id": "payment-55a4005c258c",
  "risk_score": 100,
  "risk_level": "HIGH",
  "decision": "blocked",
  "reasons": ["high_transaction_frequency", "repeated_blocked_payments",
              "country_change", "new_device"]
}
```

The escalation across the burst:

```text
payment #20   80   HIGH   blocked   (frequency + country + device)
payment #21   80   HIGH   blocked
payment #22   80   HIGH   blocked
payment #23   80   HIGH   blocked
payment #24   80   HIGH   blocked
payment #25  100   HIGH   blocked   ← five blocks now on record, +20
```

That jump from 80 to 100 is the feedback loop closing.

### It does not fire

Four blocked payments — four is not five; the rule is a threshold, not a gradient. Or six blocked
payments **six minutes ago** — the signal expires. A user blocked repeatedly last week is not
permanently condemned; this is a short-memory rule about an *ongoing* attack, not a reputation
system.

### Limitations

- **Rare by construction.** Needing five blocks inside five minutes means it only fires during an
  active, sustained attack, and contributes nothing in normal operation.
- **Blind to other accounts.** An attacker who abandons an account after two blocks and moves on
  never triggers it.
- **It compounds with itself.** Once firing, it makes further blocks more likely, which keeps it
  firing. A real system would want explicit decay or a cooldown so an account can recover.

---

## 9. Rule 4 — Country Change

**+30 points** — the second-heaviest rule.

> Is this payment from a different country than the user's previous payment, within the last hour?

### What it detects

Impossible travel. A user who paid from the US two minutes ago cannot physically be in Bangladesh
now. Either the account is being used by someone else, or a proxy is hiding who is using it.

It outscores the device rule because geography is harder to fake accidentally. People buy new
phones all the time; people do not cross oceans in two minutes.

### Configuration

```python
rule_country_window_seconds: int = 3600   # RULE_COUNTRY_WINDOW_SECONDS  (1 hour)
rule_country_score: int = 30              # RULE_COUNTRY_SCORE
```

### The code

```python
async def country_change(session, event, at, s) -> RuleHit | None:
    previous = await payments_repo.previous_payment(session, event.user_id, before=at)
    if previous is None or previous.country == event.country:
        return None
    if at - previous.created_at <= timedelta(seconds=s.rule_country_window_seconds):
        return RuleHit("country_change", s.rule_country_score)
    return None
```

Three conditions, any of which stops the rule: there must *be* a previous payment, the country must
differ, and the gap must be under an hour.

```sql
SELECT *
FROM payments
WHERE user_id = :user_id
  AND created_at < :at                 -- strictly before the scored payment
ORDER BY created_at DESC
LIMIT 1;
```

### It fires

Two payments, US then BD, on the **same device** so the device rule cannot interfere:

```json
{
  "payment_id": "payment-32b622882abc",
  "risk_score": 30,
  "risk_level": "MEDIUM",
  "decision": "approved",
  "reasons": ["country_change"]
}
```

Combined with a new device, the second payment scores 40:

```json
{
  "payment_id": "payment-0db685201e24",
  "risk_score": 40,
  "risk_level": "MEDIUM",
  "decision": "approved",
  "reasons": ["country_change", "new_device"]
}
```

Still approved, and that is the correct call — this is exactly what a real user landing abroad with
a new phone looks like. It takes velocity on top to reach 70.

### It does not fire

```text
previous US, current US        →  no hit   (nothing changed)
previous None                  →  no hit   (no baseline to change from)
previous US two days ago, BD   →  no hit   (48h > 1h — enough time to fly anywhere)
```

The rule is about *impossible* travel, not travel.

### Regression — the bug that made this rule silent

This rule exposed the most serious bug in the codebase. It did not error, did not warn, and did not
fire. The original query was:

```sql
-- WRONG
SELECT * FROM payments
WHERE user_id = :user_id AND id != :current_payment_id
ORDER BY created_at DESC
LIMIT 1;
```

*"The user's most recent payment other than this one"* — which sounds right, and is wrong, because
the detector runs **behind** the producer. During a burst, the database when payment #15 is scored:

```text
#14  10:00:14  US
#15  10:00:15  BD   ← being scored right now
#16  10:00:16  BD
#17  10:00:17  BD
#18  10:00:18  BD   ← "most recent, other than #15"
```

The query returned **#18**, a payment from the future, whose country is also BD. So
`previous.country == event.country` and the rule returned `None`. The country hop at #15 — the
actual signal — was invisible, masked by later payments *belonging to the same attack*.

The fix anchors to `created_at < :at`, so the query returns **#14** (US) and the rule fires. Verified
empirically: replaying the old query against the regression test's data reports
`country_change fires? False`.

### Why the burst alternates countries

`scripts/burst.sh` flips the country on **every** payment after #19 (`BD, US, BD, US…`). If it
switched to BD once and stayed, only payment #20 would show a change — #21 onward would compare BD
against BD and see nothing. Alternating means every payment in the attack differs from its
predecessor, so the whole tail scores 80. It is also realistic: rapidly rotating proxy exit nodes
look exactly like this.

### Limitations

- **It compares against one previous payment**, not the user's usual set of countries. A genuine
  VPN user alternating countries trips it every time.
- **No notion of distance or feasibility.** US → CA in ten minutes scores the same 30 as US → BD,
  though only one is physically impossible. Real detection uses coordinates and a plausible-velocity
  check.
- **Country is self-reported by the client.** It arrives in the request body and nothing verifies
  it. A production system derives it server-side from the IP or card BIN — any signal the client
  controls is a signal an attacker controls.

---

## 10. Rule 5 — New Device

**+10 points** — the lightest rule in the system.

> Has this user ever paid from this device before?

### What it detects

An account being used from hardware it has never been used from — what an account takeover looks
like from the server's side: same credentials, same user id, different machine.

It scores only 10 because the false-positive rate is enormous. People buy phones, reinstall
browsers, clear cookies, borrow laptops. A new device is *interesting*, not *alarming*.

### Configuration

```python
rule_new_device_score: int = 10   # RULE_NEW_DEVICE_SCORE
```

No window and no count — this rule asks about the user's entire history. A device used once two
years ago is still a device you have used.

### The code

```python
async def new_device(session, event, at, s) -> RuleHit | None:
    devices = await payments_repo.devices_seen_before(session, event.user_id, before=at)
    if devices and event.device_id not in devices:
        return RuleHit("new_device", s.rule_new_device_score)
    return None
```

```sql
SELECT device_id
FROM payments
WHERE user_id = :user_id
  AND created_at < :at;
```

There is no `DISTINCT` — rows come back with duplicates and Python's `set()` collapses them. At this
scale that is cheaper; a user with a long history would want `DISTINCT` so duplicates never cross
the wire.

### The `devices and ...` guard

That first condition is doing real work. If `devices` is empty, this is the user's **first ever
payment** — every device is new to someone who has never paid before, so firing here would score
every new customer for the crime of being new.

```text
first payment ever        devices = {}            →  no hit
second payment, same dev  devices = {"device-A"}  →  no hit
second payment, new dev   devices = {"device-A"}  →  HIT, +10
```

The same reasoning makes rule 4 return `None` when there is no previous payment. **A rule about
change needs something to change from.**

### It fires

Two payments, same country so geography cannot interfere, different devices:

```json
{
  "payment_id": "payment-e0713f0b1602",
  "risk_score": 10,
  "risk_level": "LOW",
  "decision": "approved",
  "reasons": ["new_device"]
}
```

Someone bought a new phone. The system notices and moves on — a weak signal recorded without being
acted on.

### It does not fire

```text
history: device-A, device-B, device-A
current: device-B     →  already known  →  no hit
```

Once a device is known it stays known forever. A user alternating between phone and laptop trips
this at most twice in their lifetime.

### Regression — devices from the future

Same class of bug as rule 4, from the same cause. The original query collected every device except
the current row's:

```sql
-- WRONG
SELECT device_id FROM payments
WHERE user_id = :user_id AND id != :current_payment_id;
```

When payment #15 (the first from `device-stolen`) was scored, the database already held #16–#25,
all also from `device-stolen`:

```text
devices = {"device-known", "device-stolen"}
event.device_id = "device-stolen"   →  already in the set  →  no hit
```

The stolen device counted as **known**, because the attacker's own later payments had established
it. Verified: the old query prints `OLD known_devices -> {'device-known', 'device-stolen'}` and
`new_device fires? False`.

The `created_at < :at` bound fixes it — only devices seen *before* this payment count as known.

### Why the burst uses a fresh device each time

`scripts/burst.sh` uses `device-stolen-$i` — a different id for every payment after #19. Reusing a
single `device-stolen` would mean only payment #20 scored the hit, since #21 onward would find it
already known. A fresh device per payment keeps the rule firing across the whole attack, and is
plausible: an attacker rotating through a botnet presents new fingerprints continuously.

### Limitations

- **`device_id` is self-reported.** An attacker with stolen credentials can also send the victim's
  usual device id and defeat the rule entirely. Real systems derive a fingerprint server-side from
  many weak signals and treat it as probabilistic.
- **A device is known forever.** No ageing. A device used once three years ago still counts as
  trusted.
- **It cannot distinguish "new device" from "new device id".** A browser cache clear produces a
  fresh id for the same machine — a large part of why this rule is only worth 10.

---

## 11. Seeing Which Rules Fired

The reasons travel with the decision event and land in the database:

```bash
curl localhost:8700/payments/{payment_id}/fraud
```

```json
{
  "risk_score": 100,
  "risk_level": "HIGH",
  "decision": "blocked",
  "reasons": ["high_transaction_frequency", "repeated_blocked_payments",
              "country_change", "new_device"]
}
```

They also appear in the detector's log and the audit-logger's:

```text
fraud-detector-1 | risk.assessed  payment_id=payment-55a4005c258c risk_score=100
                                  risk_level=HIGH decision=blocked
                                  reasons=['high_transaction_frequency',
                                           'repeated_blocked_payments',
                                           'country_change', 'new_device']
```

A blocked payment is never a black box — the exact set of reasons is always recorded.

---

## 12. Triggering Each Rule by Hand

```bash
# Rule 2 - large amount (works on a brand new user)
curl -X POST localhost:8700/payments -H 'content-type: application/json' \
  -d '{"user_id":"whale-1","amount":9999,"currency":"USD","country":"US","device_id":"d1"}'
```

```bash
# Rule 5 - new device: same user, same country, change only the device
curl -X POST localhost:8700/payments -H 'content-type: application/json' \
  -d '{"user_id":"dev-test","amount":25,"currency":"USD","country":"US","device_id":"phone"}'
curl -X POST localhost:8700/payments -H 'content-type: application/json' \
  -d '{"user_id":"dev-test","amount":25,"currency":"USD","country":"US","device_id":"laptop"}'
```

```bash
# Rule 4 - country change: same user, same device, change only the country
curl -X POST localhost:8700/payments -H 'content-type: application/json' \
  -d '{"user_id":"traveller","amount":25,"currency":"USD","country":"US","device_id":"d1"}'
curl -X POST localhost:8700/payments -H 'content-type: application/json' \
  -d '{"user_id":"traveller","amount":25,"currency":"USD","country":"BD","device_id":"d1"}'
```

```bash
# Rules 1 and 3 - frequency, then the repeated-blocks feedback on the last payment
./scripts/burst.sh my-test-user
```

Watch the detector reason about any of them:

```bash
make detector-logs
```

The dashboard at http://localhost:5193 can do all of the above through a form, including a
"Simulate takeover" button.

---

## 13. Tuning

Every threshold is an environment variable. To make the frequency rule far more aggressive:

```bash
RULE_FREQUENCY_COUNT=5 RULE_FREQUENCY_WINDOW_SECONDS=10 docker compose up -d fraud-detector
```

To block on MEDIUM as well:

```bash
FRAUD_THRESHOLD=30 docker compose up -d fraud-detector
```

Only the **detector** needs restarting — the API does not score anything, so it never reads these
values.

Changing a threshold does not rewrite history; existing `fraud_decisions` rows keep their scores. To
re-score everything under new thresholds:

```bash
./scripts/replay.sh fraud-detector payment.events
```

Because scoring is replay-deterministic, this is genuinely useful — it is how you would evaluate a
rule change against real historical traffic.

---

## 14. Tests

```bash
make test
```

- `tests/unit/test_risk_engine.py` — pure scoring: the bands, the threshold, the arithmetic. No
  database, runs anywhere.
- `tests/integration/test_rules.py` — the rules against real PostgreSQL, because the rules *are*
  time-window SQL and their bugs were bugs in the SQL semantics. A mock would not have caught them.

| Test | What it pins down |
|---|---|
| `test_frequency_fires_at_the_threshold` | Exactly 20 in 30s produces a +40 hit |
| `test_frequency_ignores_payments_outside_the_window` | 20 payments over an hour produce nothing |
| `test_frequency_ignores_payments_made_after_the_scored_one` | **Regression** — later payments cannot count |
| `test_country_change_compares_against_the_previous_payment` | US → BD fires for +30 |
| `test_country_change_ignores_later_payments` | **Regression** — a later BD payment must not mask the hop |
| `test_country_change_silent_when_country_is_stable` | US → US does nothing |
| `test_country_change_ignores_a_stale_previous_payment` | A two-day-old previous payment does nothing |
| `test_new_device_ignores_devices_first_seen_later` | **Regression** — a device only used later is not "known" |
| `test_first_ever_payment_is_not_a_new_device` | The empty-history guard |

The three regressions were confirmed to fail against the original queries before the fix.
