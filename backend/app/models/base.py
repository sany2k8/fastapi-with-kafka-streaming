from datetime import datetime

from sqlalchemy import DateTime, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


def utc_now_column() -> Mapped[datetime]:
    """timestamptz, always.

    Every fraud rule is a time-window query. A naive datetime compared against
    an aware one either raises or silently shifts the window - both wreck the
    rules in ways that are painful to debug.
    """
    return mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
        index=True,
    )
