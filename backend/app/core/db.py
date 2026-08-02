"""Async SQLAlchemy engine + session factory, shared by the API and the consumers."""

from collections.abc import AsyncIterator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.core.config import get_settings
from app.models.base import Base

_settings = get_settings()

engine = create_async_engine(_settings.database_url, echo=False, pool_pre_ping=True)

SessionLocal = async_sessionmaker(engine, expire_on_commit=False)


async def get_session() -> AsyncIterator[AsyncSession]:
    """FastAPI dependency."""
    async with SessionLocal() as session:
        yield session


async def init_models() -> None:
    """Create the two tables if they are missing.

    Two tables and no production data, so Alembic would be pure ceremony here.
    """
    # Imported for the side effect of registering the mappers on Base.metadata.
    from app.models import fraud_decision, payment  # noqa: F401

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
