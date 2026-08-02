from typing import Any

from fastapi import APIRouter

from app.kafka.inspect import inspect_cluster

router = APIRouter(prefix="/kafka", tags=["kafka"])


@router.get("/inspect")
async def inspect() -> dict[str, Any]:
    """Topics, partitions, end offsets, per-group committed offsets and lag.

    The whole vocabulary of Kafka as one JSON document. Cross-check it against
    Kafka UI on http://localhost:8092 (`make ui`).
    """
    return await inspect_cluster()
