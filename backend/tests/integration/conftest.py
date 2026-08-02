"""Fixtures for tests that need the real Postgres from compose.

Run `make up` first. These use the real database on purpose: the rules are
time-window SQL, and the bugs they had were bugs in the SQL semantics, not in
the Python around it. A fake would not have caught them.
"""

import os
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.models.base import Base
from app.models.fraud_decision import FraudDecision  # noqa: F401  (registers the mapper)
from app.models.payment import Payment  # noqa: F401

TEST_DATABASE_URL = os.environ.get(
    "TEST_DATABASE_URL", "postgresql+asyncpg://fraud:fraud@localhost:5436/fraud"
)


@pytest_asyncio.fixture
async def session() -> AsyncIterator[AsyncSession]:
    engine = create_async_engine(TEST_DATABASE_URL)
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
    except Exception as exc:  # pragma: no cover - environment guard
        pytest.skip(f"Postgres not reachable ({exc}). Run `make up` first.")

    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as s:
        yield s
    await engine.dispose()
