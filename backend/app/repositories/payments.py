"""All payment SQL lives here. Services and consumers never write queries."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.payment import Payment


async def create(
    session: AsyncSession,
    *,
    payment_id: str,
    user_id: str,
    amount: Decimal,
    currency: str,
    country: str,
    device_id: str,
) -> Payment:
    payment = Payment(
        id=payment_id,
        user_id=user_id,
        amount=amount,
        currency=currency,
        country=country,
        device_id=device_id,
        status="processing",
        created_at=datetime.now(UTC),
    )
    session.add(payment)
    await session.commit()
    return payment


async def get(session: AsyncSession, payment_id: str) -> Payment | None:
    return await session.get(Payment, payment_id)


async def list_recent(session: AsyncSession, *, user_id: str | None, limit: int) -> list[Payment]:
    stmt = select(Payment).order_by(Payment.created_at.desc()).limit(limit)
    if user_id:
        stmt = stmt.where(Payment.user_id == user_id)
    return list((await session.scalars(stmt)).all())


async def set_status(session: AsyncSession, payment_id: str, status: str) -> None:
    payment = await session.get(Payment, payment_id)
    if payment is not None:
        payment.status = status
        await session.commit()


# --- Queries the fraud rules depend on --------------------------------------


# Every one of these takes an explicit `at` boundary - the created_at of the
# payment being scored - instead of using "now".
#
# That matters more than it looks. The detector runs behind the producer, so by
# the time payment #15 of a burst is scored, payments #16-#20 are already in the
# table. Asking "what is this user's most recent payment?" would return #20 and
# the country-change rule would compare BD against BD and see nothing wrong.
# Anchoring to the payment's own timestamp asks the right question - "what did
# this user's history look like at the moment of this payment?" - and makes the
# score **deterministic**: replay the same event tomorrow and rules 1, 2, 4 and 5
# produce the identical result. With at-least-once delivery, replays happen.


async def count_in_window(
    session: AsyncSession, user_id: str, *, seconds: int, at: datetime
) -> int:
    """Payments by this user in the `seconds` leading up to `at` (inclusive)."""
    since = at - timedelta(seconds=seconds)
    stmt = select(func.count()).where(
        Payment.user_id == user_id,
        Payment.created_at >= since,
        Payment.created_at <= at,
    )
    return int((await session.scalar(stmt)) or 0)


async def count_blocked_in_window(
    session: AsyncSession, user_id: str, *, seconds: int, at: datetime
) -> int:
    """Blocked payments by this user shortly before `at`.

    The one rule that is NOT replay-deterministic: `status` is written
    asynchronously by decision-writer, so a payment scored a moment ago may not
    be marked `blocked` yet. That is normal eventual consistency, and the rule
    is a slow-moving signal, so it is fine here.
    """
    since = at - timedelta(seconds=seconds)
    stmt = select(func.count()).where(
        Payment.user_id == user_id,
        Payment.status == "blocked",
        Payment.created_at >= since,
        Payment.created_at <= at,
    )
    return int((await session.scalar(stmt)) or 0)


async def previous_payment(
    session: AsyncSession, user_id: str, *, before: datetime
) -> Payment | None:
    """The user's last payment strictly before `before`."""
    stmt = (
        select(Payment)
        .where(Payment.user_id == user_id, Payment.created_at < before)
        .order_by(Payment.created_at.desc())
        .limit(1)
    )
    return await session.scalar(stmt)


async def devices_seen_before(session: AsyncSession, user_id: str, *, before: datetime) -> set[str]:
    stmt = select(Payment.device_id).where(Payment.user_id == user_id, Payment.created_at < before)
    return set((await session.scalars(stmt)).all())


async def list_stuck(session: AsyncSession, *, older_than_seconds: int) -> list[Payment]:
    """Payments still `processing` well after they were created.

    Known gap: the API writes the row and then publishes to Kafka. Those two
    steps are not atomic (a *dual write*). If the process dies in between, the
    row is stranded at `processing` and no event exists. The production fix is
    a transactional outbox, which is out of scope here - so instead the gap is
    made *visible*, which is better than pretending it does not exist.
    """
    cutoff = datetime.now(UTC) - timedelta(seconds=older_than_seconds)
    stmt = (
        select(Payment)
        .where(Payment.status == "processing", Payment.created_at < cutoff)
        .order_by(Payment.created_at.desc())
    )
    return list((await session.scalars(stmt)).all())
