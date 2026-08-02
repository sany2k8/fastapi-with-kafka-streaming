from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api import kafka_admin, payments
from app.core.db import init_models
from app.core.logging import configure_logging, get_logger
from app.kafka.producer import producer
from app.kafka.topics import ensure_topics

log = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    configure_logging()
    await init_models()
    # Topics are created here, once, with an explicit partition count - before
    # any consumer subscribes.
    await ensure_topics()
    await producer.start()
    log.info("api.ready")
    yield
    await producer.stop()


app = FastAPI(
    title="Real-Time Fraud Detection",
    description=(
        "POST /payments returns immediately with `processing`. Fraud scoring "
        "happens asynchronously in a Kafka consumer. See /kafka/inspect for "
        "topics, partitions, offsets and consumer lag."
    ),
    version="0.1.0",
    lifespan=lifespan,
)

# Local dashboard only.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5193"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(payments.router)
app.include_router(kafka_admin.router)


@app.get("/health", tags=["meta"])
async def health() -> dict[str, str]:
    return {"status": "ok"}
