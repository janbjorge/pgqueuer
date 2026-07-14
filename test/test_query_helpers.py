from __future__ import annotations

import random
from datetime import timedelta
from itertools import count

from pgqueuer.adapters.persistence.query_helpers import (
    NormedEnqueueParam,
    normalize_enqueue_params,
    scatter_ids_by_ordinal,
)


def test_normalize_single_entrypoint() -> None:
    result = normalize_enqueue_params(
        entrypoint="task1",
        payload=b"data1",
        priority=1,
        execute_after=timedelta(seconds=10),
        dedupe_key="key",
    )
    expected = NormedEnqueueParam(
        priority=[1],
        entrypoint=["task1"],
        payload=[b"data1"],
        execute_after=[timedelta(seconds=10)],
        dedupe_key=["key"],
        headers=[None],
    )
    assert result == expected


def test_normalize_multiple_entrypoints() -> None:
    result = normalize_enqueue_params(
        entrypoint=["task1", "task2"],
        payload=[b"data1", b"data2"],
        priority=[1, 2],
        execute_after=[timedelta(seconds=10), timedelta(seconds=20)],
        dedupe_key=["k1", "k2"],
    )
    expected = NormedEnqueueParam(
        priority=[1, 2],
        entrypoint=["task1", "task2"],
        payload=[b"data1", b"data2"],
        execute_after=[timedelta(seconds=10), timedelta(seconds=20)],
        dedupe_key=["k1", "k2"],
        headers=[None, None],
    )
    assert result == expected


def test_normalize_single_entrypoint_no_execute_after() -> None:
    result = normalize_enqueue_params(
        entrypoint="task1",
        payload=b"data1",
        priority=1,
    )
    expected = NormedEnqueueParam(
        priority=[1],
        entrypoint=["task1"],
        payload=[b"data1"],
        execute_after=[timedelta(seconds=0)],
        dedupe_key=[None],
        headers=[None],
    )
    assert result == expected


def test_normalize_single_entrypoint_no_dedupe_key() -> None:
    result = normalize_enqueue_params(
        entrypoint="task1",
        payload=b"data1",
        priority=1,
    )
    expected = NormedEnqueueParam(
        priority=[1],
        entrypoint=["task1"],
        payload=[b"data1"],
        execute_after=[timedelta(seconds=0)],
        dedupe_key=[None],
        headers=[None],
    )
    assert result == expected


def test_normalize_multiple_entrypoints_no_execute_after() -> None:
    result = normalize_enqueue_params(
        entrypoint=["task1", "task2"],
        payload=[b"data1", b"data2"],
        priority=[1, 2],
    )
    expected = NormedEnqueueParam(
        priority=[1, 2],
        entrypoint=["task1", "task2"],
        payload=[b"data1", b"data2"],
        execute_after=[timedelta(seconds=0), timedelta(seconds=0)],
        dedupe_key=[None, None],
        headers=[None, None],
    )
    assert result == expected


def test_normalize_mixed_execute_after() -> None:
    result = normalize_enqueue_params(
        entrypoint=["task1", "task2"],
        payload=[b"data1", b"data2"],
        priority=[1, 2],
        execute_after=[timedelta(seconds=10), None],
    )
    expected = NormedEnqueueParam(
        priority=[1, 2],
        entrypoint=["task1", "task2"],
        payload=[b"data1", b"data2"],
        execute_after=[timedelta(seconds=10), timedelta(seconds=0)],
        dedupe_key=[None, None],
        headers=[None, None],
    )
    assert result == expected


def test_normalize_mixed_dedupe_key() -> None:
    result = normalize_enqueue_params(
        entrypoint=["task1", "task2"],
        payload=[b"data1", b"data2"],
        priority=[1, 2],
        dedupe_key=["foo", "bar"],
    )
    expected = NormedEnqueueParam(
        priority=[1, 2],
        entrypoint=["task1", "task2"],
        payload=[b"data1", b"data2"],
        execute_after=[timedelta(seconds=0), timedelta(seconds=0)],
        dedupe_key=["foo", "bar"],
        headers=[None, None],
    )
    assert result == expected


def test_normalize_single_entrypoint_none_payload() -> None:
    result = normalize_enqueue_params(
        entrypoint="task1",
        payload=None,
        priority=1,
        execute_after=timedelta(seconds=10),
        dedupe_key="foo",
    )
    expected = NormedEnqueueParam(
        priority=[1],
        entrypoint=["task1"],
        payload=[None],
        execute_after=[timedelta(seconds=10)],
        dedupe_key=["foo"],
        headers=[None],
    )
    assert result == expected


def test_normalize_multiple_entrypoints_none_payload() -> None:
    result = normalize_enqueue_params(
        entrypoint=["task1", "task2"],
        payload=[None, b"data2"],
        priority=[1, 2],
        execute_after=[timedelta(seconds=10), timedelta(seconds=20)],
        dedupe_key=["foo", "bar"],
    )
    expected = NormedEnqueueParam(
        priority=[1, 2],
        entrypoint=["task1", "task2"],
        payload=[None, b"data2"],
        execute_after=[timedelta(seconds=10), timedelta(seconds=20)],
        dedupe_key=["foo", "bar"],
        headers=[None, None],
    )
    assert result == expected


def test_normalize_headers_dict() -> None:
    result = normalize_enqueue_params(
        entrypoint="task1",
        payload=None,
        priority=1,
        headers={"trace": "123"},
    )
    expected = NormedEnqueueParam(
        priority=[1],
        entrypoint=["task1"],
        payload=[None],
        execute_after=[timedelta(seconds=0)],
        dedupe_key=[None],
        headers=[{"trace": "123"}],
    )
    assert result == expected


def rows(*ord_id_pairs: tuple[int, int]) -> list[dict]:
    """Build inserted rows carrying the input ordinal and assigned id."""
    return [{"ord": o, "id": i} for o, i in ord_id_pairs]


def test_scatter_all_inserted() -> None:
    assert scatter_ids_by_ordinal(rows((1, 101), (2, 102)), 2) == [101, 102]


def test_scatter_skipped_middle() -> None:
    assert scatter_ids_by_ordinal(rows((1, 101), (3, 103)), 3) == [101, None, 103]


def test_scatter_all_skipped() -> None:
    assert scatter_ids_by_ordinal([], 2) == [None, None]


def test_scatter_empty_batch() -> None:
    assert scatter_ids_by_ordinal([], 0) == []


def test_scatter_ignores_row_order() -> None:
    # The result is keyed on `ord`, not on the order rows arrive in.
    assert scatter_ids_by_ordinal(rows((3, 103), (1, 101)), 3) == [101, None, 103]


def test_scatter_first_and_last_skipped() -> None:
    assert scatter_ids_by_ordinal(rows((2, 202)), 3) == [None, 202, None]


def simulate_enqueue(
    dedupe_keys: list[str | None],
    active: set[str],
) -> tuple[list[dict], list[int | None]]:
    """Reference model of the skip-mode INSERT.

    Mirrors ON CONFLICT DO NOTHING against the *active* key set (null keys
    never conflict, an inserted key becomes active for the rest of the batch).
    Each surviving row carries its 1-based input ordinal and a fresh id.
    """
    inserted = list[dict]()
    expected = list[int | None]()
    remaining = set(active)
    next_id = count(1)
    for ordinal, key in enumerate(dedupe_keys, start=1):
        if key is not None and key in remaining:
            expected.append(None)
            continue
        job_id = next(next_id)
        inserted.append({"ord": ordinal, "id": job_id})
        expected.append(job_id)
        if key is not None:
            remaining.add(key)
    return inserted, expected


def test_scatter_matches_simulated_insert_semantics() -> None:
    rng = random.Random(678)
    alphabet = ["a", "b", "c", None]
    for _ in range(1000):
        keys = [rng.choice(alphabet) for _ in range(rng.randint(0, 12))]
        active = {key for key in "abc" if rng.random() < 0.4}
        inserted, expected = simulate_enqueue(keys, active)
        assert scatter_ids_by_ordinal(inserted, len(keys)) == expected
