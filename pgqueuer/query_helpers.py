from datetime import timedelta
from typing import Sequence


def normalize_enqueue_params(
    entrypoint: str | list[str],
    payload: bytes | None | list[bytes | None],
    priority: int | list[int],
    execute_after: timedelta | None | list[timedelta | None] = None,
) -> tuple[list[str], list[bytes | None], list[int], list[timedelta]]:
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

    return normed_priority, normed_entrypoint, normed_payload, normed_execute_after
