"""structlog setup.

Every consumer logs topic / partition / offset on each message it handles.
Reading those three fields as events flow is the fastest way to internalise
how Kafka actually delivers work.
"""

import logging
import sys

import structlog

from app.core.config import get_settings


def configure_logging() -> None:
    settings = get_settings()

    renderer: structlog.types.Processor = (
        structlog.processors.JSONRenderer()
        if settings.log_json
        else structlog.dev.ConsoleRenderer(colors=False)
    )

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="%H:%M:%S", utc=True),
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=True,
    )

    # aiokafka is chatty about group coordination at DEBUG; INFO is the sweet
    # spot - you still see "Joined group ... generation N" during rebalances.
    logging.basicConfig(level=logging.INFO, format="%(name)s %(message)s", stream=sys.stdout)
    logging.getLogger("aiokafka.consumer.group_coordinator").setLevel(logging.INFO)


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    return structlog.get_logger(name)  # type: ignore[no-any-return]
