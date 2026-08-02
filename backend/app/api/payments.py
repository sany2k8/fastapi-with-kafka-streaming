import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.core.db import get_session
from app.kafka.producer import producer
from app.models.fraud_decision import FraudDecision
from app.models.payment import Payment
from app.repositories import fraud_decisions as decisions_repo
from app.repositories import payments as payments_repo
from app.schemas.events import PaymentCreatedEvent
from app.schemas.payment import FraudDecisionOut, PaymentAccepted, PaymentCreate, PaymentOut

router = APIRouter(tags=["payments"])

SessionDep = Annotated[AsyncSession, Depends(get_session)]


@router.post("/payments", response_model=PaymentAccepted, status_code=status.HTTP_202_ACCEPTED)
async def create_payment(body: PaymentCreate, session: SessionDep) -> PaymentAccepted:
    """Accept a payment and hand it off to Kafka.

    Note what this endpoint does NOT do: it never scores, never loads history,
    never waits for the detector. It writes the row, publishes one event, and
    returns 202 with `processing`. That is the whole point of the exercise -
    the API's latency is independent of how slow fraud analysis gets.
    """
    settings = get_settings()
    payment_id = f"payment-{uuid.uuid4().hex[:12]}"

    await payments_repo.create(
        session,
        payment_id=payment_id,
        user_id=body.user_id,
        amount=body.amount,
        currency=body.currency,
        country=body.country,
        device_id=body.device_id,
    )

    event = PaymentCreatedEvent(
        event_id=f"evt-{uuid.uuid4().hex[:12]}",
        payment_id=payment_id,
        user_id=body.user_id,
        amount=body.amount,
        currency=body.currency,
        country=body.country,
        device_id=body.device_id,
    )
    # key=user_id - see EventProducer.publish for why this matters so much.
    await producer.publish(settings.topic_payment_events, key=body.user_id, event=event)

    return PaymentAccepted(payment_id=payment_id)


@router.get("/payments", response_model=list[PaymentOut])
async def list_payments(
    session: SessionDep,
    user_id: str | None = None,
    limit: int = Query(50, le=200),
) -> list[PaymentOut]:
    """Recent payments, newest first - what the dashboard polls."""
    rows = await payments_repo.list_recent(session, user_id=user_id, limit=limit)
    out = []
    for payment in rows:
        decision = await decisions_repo.get_by_payment(session, payment.id)
        out.append(_to_out(payment, decision))
    return out


# Declared BEFORE /payments/{payment_id}: FastAPI matches routes in order, so a
# literal path registered after a parameterised one is unreachable - "stuck"
# would be swallowed as a payment_id.
@router.get("/payments/stuck", response_model=list[PaymentOut])
async def stuck_payments(session: SessionDep, older_than_seconds: int = 60) -> list[PaymentOut]:
    """Payments still `processing` long after creation.

    Makes the known dual-write gap observable rather than invisible.
    """
    rows = await payments_repo.list_stuck(session, older_than_seconds=older_than_seconds)
    return [_to_out(payment, None) for payment in rows]


@router.get("/payments/{payment_id}", response_model=PaymentOut)
async def get_payment(payment_id: str, session: SessionDep) -> PaymentOut:
    payment = await payments_repo.get(session, payment_id)
    if payment is None:
        raise HTTPException(status_code=404, detail="payment not found")
    decision = await decisions_repo.get_by_payment(session, payment_id)
    return _to_out(payment, decision)


@router.get("/payments/{payment_id}/fraud", response_model=FraudDecisionOut)
async def get_fraud_decision(payment_id: str, session: SessionDep) -> FraudDecisionOut:
    decision = await decisions_repo.get_by_payment(session, payment_id)
    if decision is None:
        raise HTTPException(
            status_code=404,
            detail="no decision yet - the payment is still being analysed",
        )
    return FraudDecisionOut(
        payment_id=decision.payment_id,
        user_id=decision.user_id,
        risk_score=decision.risk_score,
        risk_level=decision.risk_level,
        decision=decision.decision,
        reasons=decision.reasons,
        created_at=decision.created_at,
    )


def _to_out(payment: Payment, decision: FraudDecision | None) -> PaymentOut:
    """Merge the payment row with its decision, if one has been made yet.

    `risk_score` is None for a payment still in flight - the API genuinely does
    not know it yet, and saying so is more honest than defaulting to 0.
    """
    return PaymentOut(
        payment_id=payment.id,
        user_id=payment.user_id,
        amount=payment.amount,
        currency=payment.currency,
        country=payment.country,
        device_id=payment.device_id,
        status=payment.status,
        created_at=payment.created_at,
        risk_score=decision.risk_score if decision else None,
        risk_level=decision.risk_level if decision else None,
    )
