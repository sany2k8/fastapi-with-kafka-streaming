"""Rule tests, including regressions for the two bugs these rules shipped with.

The original rules asked "what is this user's most recent payment?". Because
the detector runs *behind* the producer, by the time payment #15 of a burst is
scored, payments #16-#25 are already in the table - so "most recent" was a
payment from the future, and the country/device rules silently never fired.

Every rule is now anchored to the scored payment's own created_at.
"""

import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings
from app.fraud.rules import country_change, new_device, transaction_frequency
from app.models.payment import Payment
from app.schemas.events import PaymentCreatedEvent

SETTINGS = Settings()


async def _add(
    session: AsyncSession,
    *,
    user_id: str,
    at: datetime,
    country: str = "US",
    device_id: str = "device-known",
    status: str = "approved",
    amount: Decimal = Decimal("10.00"),
) -> Payment:
    payment = Payment(
        id=f"payment-{uuid.uuid4().hex[:12]}",
        user_id=user_id,
        amount=amount,
        currency="USD",
        country=country,
        device_id=device_id,
        status=status,
        created_at=at,
    )
    session.add(payment)
    await session.commit()
    return payment


def _event(payment: Payment) -> PaymentCreatedEvent:
    return PaymentCreatedEvent(
        event_id=f"evt-{uuid.uuid4().hex[:8]}",
        payment_id=payment.id,
        user_id=payment.user_id,
        amount=payment.amount,
        currency=payment.currency,
        country=payment.country,
        device_id=payment.device_id,
        timestamp=payment.created_at,
    )


@pytest.fixture
def user_id() -> str:
    # A fresh user per test, so the shared database stays usable.
    return f"test-{uuid.uuid4().hex[:10]}"


async def test_frequency_fires_at_the_threshold(session: AsyncSession, user_id: str) -> None:
    now = datetime.now(UTC)
    for i in range(19):
        await _add(session, user_id=user_id, at=now - timedelta(seconds=20 - i * 0.5))
    scored = await _add(session, user_id=user_id, at=now)

    hit = await transaction_frequency(session, _event(scored), now, SETTINGS)

    assert hit is not None, "20 payments inside 30s should trip rule 1"
    assert hit.score == 40


async def test_frequency_ignores_payments_outside_the_window(
    session: AsyncSession, user_id: str
) -> None:
    now = datetime.now(UTC)
    # Same 20 payments, but spread over an hour instead of 30 seconds.
    for i in range(19):
        await _add(session, user_id=user_id, at=now - timedelta(minutes=3 * (i + 1)))
    scored = await _add(session, user_id=user_id, at=now)

    assert await transaction_frequency(session, _event(scored), now, SETTINGS) is None


async def test_frequency_ignores_payments_made_after_the_scored_one(
    session: AsyncSession, user_id: str
) -> None:
    """Regression: the detector lags, so later payments already exist."""
    now = datetime.now(UTC)
    scored = await _add(session, user_id=user_id, at=now - timedelta(seconds=10))
    for i in range(30):
        await _add(session, user_id=user_id, at=now - timedelta(seconds=9 - i * 0.2))

    hit = await transaction_frequency(session, _event(scored), scored.created_at, SETTINGS)

    assert hit is None, "only history up to the scored payment may count"


async def test_country_change_compares_against_the_previous_payment(
    session: AsyncSession, user_id: str
) -> None:
    now = datetime.now(UTC)
    await _add(session, user_id=user_id, at=now - timedelta(minutes=2), country="US")
    scored = await _add(session, user_id=user_id, at=now, country="BD")

    hit = await country_change(session, _event(scored), scored.created_at, SETTINGS)

    assert hit is not None
    assert hit.score == 30


async def test_country_change_ignores_later_payments(session: AsyncSession, user_id: str) -> None:
    """Regression: 'most recent payment' used to return one from the future.

    Here the scored payment is the FIRST hop to BD, so the rule must fire -
    even though a later BD payment already sits in the table.
    """
    now = datetime.now(UTC)
    await _add(session, user_id=user_id, at=now - timedelta(minutes=2), country="US")
    scored = await _add(session, user_id=user_id, at=now - timedelta(seconds=30), country="BD")
    await _add(session, user_id=user_id, at=now, country="BD")

    hit = await country_change(session, _event(scored), scored.created_at, SETTINGS)

    assert hit is not None, "the later BD payment must not mask the hop"


async def test_country_change_silent_when_country_is_stable(
    session: AsyncSession, user_id: str
) -> None:
    now = datetime.now(UTC)
    await _add(session, user_id=user_id, at=now - timedelta(minutes=2), country="US")
    scored = await _add(session, user_id=user_id, at=now, country="US")

    assert await country_change(session, _event(scored), scored.created_at, SETTINGS) is None


async def test_country_change_ignores_a_stale_previous_payment(
    session: AsyncSession, user_id: str
) -> None:
    now = datetime.now(UTC)
    # Two days ago - travelling is not suspicious over that gap.
    await _add(session, user_id=user_id, at=now - timedelta(days=2), country="US")
    scored = await _add(session, user_id=user_id, at=now, country="BD")

    assert await country_change(session, _event(scored), scored.created_at, SETTINGS) is None


async def test_new_device_ignores_devices_first_seen_later(
    session: AsyncSession, user_id: str
) -> None:
    """Regression: a device used only in *later* payments used to count as known."""
    now = datetime.now(UTC)
    await _add(session, user_id=user_id, at=now - timedelta(minutes=1), device_id="device-known")
    scored = await _add(
        session, user_id=user_id, at=now - timedelta(seconds=30), device_id="device-stolen"
    )
    await _add(session, user_id=user_id, at=now, device_id="device-stolen")

    hit = await new_device(session, _event(scored), scored.created_at, SETTINGS)

    assert hit is not None
    assert hit.score == 10


async def test_first_ever_payment_is_not_a_new_device(session: AsyncSession, user_id: str) -> None:
    scored = await _add(session, user_id=user_id, at=datetime.now(UTC), device_id="device-first")

    assert await new_device(session, _event(scored), scored.created_at, SETTINGS) is None
