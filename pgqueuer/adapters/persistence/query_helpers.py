from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Generator, Sequence

from pgqueuer.domain.types import JobId


@dataclass
class NormedEnqueueParam:
    priority: list[int]
    entrypoint: list[str]
    payload: list[bytes | None]
    execute_after: list[timedelta]
    dedupe_key: list[str | None]
    headers: list[dict | None]


def normalize_enqueue_params(
    entrypoint: str | list[str],
    payload: bytes | None | list[bytes | None],
    priority: int | list[int],
    execute_after: timedelta | None | list[timedelta | None] = None,
    dedupe_key: str | list[str | None] | None = None,
    headers: dict | list[dict | None] | None = None,
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

    headers = [None] * len(normed_entrypoint) if headers is None else headers
    normed_headers = headers if isinstance(headers, list) else [headers]

    return NormedEnqueueParam(
        priority=normed_priority,
        entrypoint=normed_entrypoint,
        payload=normed_payload,
        execute_after=normed_execute_after,
        dedupe_key=normed_dedupe_key,
        headers=normed_headers,
    )


def align_ids_with_dedupe_keys(
    rows: list[dict],
    dedupe_keys: list[str | None],
) -> list[JobId | None]:
    """Map inserted (id, dedupe_key) rows back onto input positions.

    Rows arrive in input order (skipped duplicates absent). A skipped input's
    key never appears in *rows* — the conflicting job predates the batch, and a
    within-batch duplicate is consumed by its first occurrence — so a row
    belongs to the current input position iff the keys match.
    """
    ids = list[JobId | None]()
    pending = iter(rows)
    row = next(pending, None)
    for key in dedupe_keys:
        if row is not None and row["dedupe_key"] == key:
            ids.append(JobId(row["id"]))
            row = next(pending, None)
        else:
            ids.append(None)
    return ids


def merge_tracing_headers(
    headers: list[dict | None],
    trace_headers: Generator[dict | None, None, None],
) -> list[dict]:
    """Merge tracing headers into the existing headers for each entrypoint."""
    return [
        {**(h or {}), **(t or {})}
        for h, t in zip(
            headers,
            trace_headers,
            strict=True,
        )
    ]
