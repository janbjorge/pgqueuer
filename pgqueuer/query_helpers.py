from dataclasses import dataclass
from datetime import timedelta
from typing import Sequence


@dataclass
class NormedEnqueueParam:
    priority: list[int]
    entrypoint: list[str]
    payload: list[bytes | None]
    execute_after: list[timedelta]
    dedupe_key: list[str | None]


def normalize_enqueue_params(
    entrypoint: str | list[str],
    payload: bytes | None | list[bytes | None],
    priority: int | list[int],
    execute_after: timedelta | None | list[timedelta | None] = None,
    dedupe_key: str | list[str | None] | None = None,
) -> NormedEnqueueParam:
    """Normalize parameters for enqueue operations to handle both single and batch inputs."""
    normed_entrypoint = entrypoint if isinstance(entrypoint, list) else [entrypoint]
    normed_payload = payload if isinstance(payload, list) else [payload]
    normed_priority = priority if isinstance(priority, list) else [priority]

    execute_after = (
        [timedelta(seconds=0)] * len(normed_entrypoint) if execute_after is None else execute_after
    )

    normed_execute_after = (
        [x or timedelta(seconds=0) for x in execute_after]
        if isinstance(execute_after, Sequence)
        else [execute_after or timedelta(seconds=0)]
    )

    dedupe_key = [None] * len(normed_entrypoint) if dedupe_key is None else dedupe_key
    normed_dedupe_key = dedupe_key if isinstance(dedupe_key, list) else [dedupe_key]

    return NormedEnqueueParam(
        priority=normed_priority,
        entrypoint=normed_entrypoint,
        payload=normed_payload,
        execute_after=normed_execute_after,
        dedupe_key=normed_dedupe_key,
    )
