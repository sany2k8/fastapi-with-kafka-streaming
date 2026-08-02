from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.fraud_decision import FraudDecision


async def insert_if_absent(
    session: AsyncSession,
    *,
    decision_id: str,
    payment_id: str,
    user_id: str,
    risk_score: int,
    risk_level: str,
    decision: str,
    reasons: list[str],
) -> bool:
    """Insert a decision, ignoring a repeat of one already stored.

    Returns True if a row was written, False if this payment already had a
    decision (i.e. the event was a redelivery).

    This one clause is what makes the whole consumer idempotent, and it is why
    at-least-once delivery is safe here: processing the same event twice
    produces exactly the same database state as processing it once.
    """
    stmt = (
        insert(FraudDecision)
        .values(
            id=decision_id,
            payment_id=payment_id,
            user_id=user_id,
            risk_score=risk_score,
            risk_level=risk_level,
            decision=decision,
            reasons=reasons,
        )
        .on_conflict_do_nothing(index_elements=["payment_id"])
        .returning(FraudDecision.id)
    )
    written = await session.scalar(stmt)
    await session.commit()
    return written is not None


async def get_by_payment(session: AsyncSession, payment_id: str) -> FraudDecision | None:
    stmt = select(FraudDecision).where(FraudDecision.payment_id == payment_id)
    return await session.scalar(stmt)
