"""The dequeue statement, rendered once per concurrency-gate shape."""

from __future__ import annotations

import dataclasses
from datetime import timedelta
from enum import Enum
from typing import NamedTuple

from pgqueuer.domain.types import QueueEntrypoint, QueueManagerId


class DequeueShape(Enum):
    """The concurrency gates a dequeue statement renders."""

    no_gates = "no_gates"
    entrypoint_gate = "entrypoint_gate"
    global_gate = "global_gate"
    both_gates = "both_gates"


@dataclasses.dataclass(frozen=True, kw_only=True)
class DequeueArgs:
    """The values one dequeue call renders and binds."""

    batch_size: int
    entrypoints: list[QueueEntrypoint]
    concurrency_limits: list[int]
    queue_manager_id: QueueManagerId
    global_concurrency_limit: int | None
    heartbeat_timeout: timedelta

    def __post_init__(self) -> None:
        if len(self.entrypoints) != len(self.concurrency_limits):
            raise ValueError("entrypoints and concurrency_limits must be the same length")
        # batch_size is rendered into the SQL text, where a bool would render as True.
        if isinstance(self.batch_size, bool) or self.batch_size < 1:
            raise ValueError("batch_size must be a positive integer")


class DequeueCacheKey(NamedTuple):
    shape: DequeueShape
    batch_size: int


def dequeue_shape(args: DequeueArgs) -> DequeueShape:
    """Select the gates a call needs; an entrypoint limit of 0 means unlimited."""
    entrypoint_gated = any(limit > 0 for limit in args.concurrency_limits)
    budget_gated = args.global_concurrency_limit is not None
    if entrypoint_gated and budget_gated:
        return DequeueShape.both_gates
    if entrypoint_gated:
        return DequeueShape.entrypoint_gate
    if budget_gated:
        return DequeueShape.global_gate
    return DequeueShape.no_gates
