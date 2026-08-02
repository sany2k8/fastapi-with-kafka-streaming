"""The five fraud rules.

Each rule is an independent async function: given the payment being scored,
the moment it happened, and the user's history up to that moment, return a
RuleHit or None. Adding a sixth rule means writing one function and adding it
to ALL_RULES - nothing else in the system changes.

`at` is always the payment's own created_at, never "now". See the comment in
repositories/payments.py for why that distinction decides whether these rules
work at all.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings
from app.repositories import payments as payments_repo
from app.schemas.events import PaymentCreatedEvent


@dataclass(frozen=True)
class RuleHit:
    reason: str
    score: int


async def transaction_frequency(
    session: AsyncSession, event: PaymentCreatedEvent, at: datetime, s: Settings
) -> RuleHit | None:
    """Rule 1 - too many payments in a short window (+40).

    The count includes the payment being scored, since the API writes the row
    before it publishes the event.
    """
    count = await payments_repo.count_in_window(
        session, event.user_id, seconds=s.rule_frequency_window_seconds, at=at
    )
    if count >= s.rule_frequency_count:
        return RuleHit("high_transaction_frequency", s.rule_frequency_score)
    return None


async def large_amount(
    session: AsyncSession, event: PaymentCreatedEvent, at: datetime, s: Settings
) -> RuleHit | None:
    """Rule 2 - unusually large single payment (+20). No history needed."""
    if float(event.amount) > s.rule_large_amount:
        return RuleHit("large_amount", s.rule_large_amount_score)
    return None


async def repeated_blocks(
    session: AsyncSession, event: PaymentCreatedEvent, at: datetime, s: Settings
) -> RuleHit | None:
    """Rule 3 - several recently blocked payments by the same user (+20).

    The PRD called this "failed payments", but nothing in this system ever
    produces a `failed` status, so the rule could never have fired. `blocked`
    is the equivalent signal here, and it makes past fraud decisions feed back
    into future scoring - which is what a real system does anyway.
    """
    count = await payments_repo.count_blocked_in_window(
        session, event.user_id, seconds=s.rule_blocked_window_seconds, at=at
    )
    if count >= s.rule_blocked_count:
        return RuleHit("repeated_blocked_payments", s.rule_blocked_score)
    return None


async def country_change(
    session: AsyncSession, event: PaymentCreatedEvent, at: datetime, s: Settings
) -> RuleHit | None:
    """Rule 4 - country differs from the previous payment, recently (+30)."""
    previous = await payments_repo.previous_payment(session, event.user_id, before=at)
    if previous is None or previous.country == event.country:
        return None
    if at - previous.created_at <= timedelta(seconds=s.rule_country_window_seconds):
        return RuleHit("country_change", s.rule_country_score)
    return None


async def new_device(
    session: AsyncSession, event: PaymentCreatedEvent, at: datetime, s: Settings
) -> RuleHit | None:
    """Rule 5 - a device this user has never paid from before (+10).

    Skipped for a user's very first payment: every device is new then, and
    scoring a first-time user for it would be nonsense.
    """
    devices = await payments_repo.devices_seen_before(session, event.user_id, before=at)
    if devices and event.device_id not in devices:
        return RuleHit("new_device", s.rule_new_device_score)
    return None


ALL_RULES = (
    transaction_frequency,
    large_amount,
    repeated_blocks,
    country_change,
    new_device,
)
