from __future__ import annotations

from datetime import timedelta

from pgqueuer.adapters.persistence.query_helpers import (
    NormedEnqueueParam,
    align_ids_with_dedupe_keys,
    normalize_enqueue_params,
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


def rows(*id_key_pairs: tuple[int, str | None]) -> list[dict]:
    return [{"id": i, "dedupe_key": k} for i, k in id_key_pairs]


def test_align_all_inserted() -> None:
    assert align_ids_with_dedupe_keys(rows((1, "a"), (2, "b")), ["a", "b"]) == [1, 2]


def test_align_null_keys_always_insert() -> None:
    assert align_ids_with_dedupe_keys(rows((1, None), (2, None)), [None, None]) == [1, 2]


def test_align_skipped_duplicate() -> None:
    assert align_ids_with_dedupe_keys(rows((1, "a"), (2, "c")), ["a", "b", "c"]) == [1, None, 2]


def test_align_all_skipped() -> None:
    assert align_ids_with_dedupe_keys([], ["a", "b"]) == [None, None]


def test_align_within_batch_duplicate_first_occurrence_wins() -> None:
    assert align_ids_with_dedupe_keys(rows((1, "a")), ["a", "a"]) == [1, None]


def test_align_skipped_key_then_null_key() -> None:
    assert align_ids_with_dedupe_keys(rows((1, None)), ["a", None]) == [None, 1]


def test_align_interleaved_null_and_skipped_keys() -> None:
    assert align_ids_with_dedupe_keys(
        rows((1, None), (2, None), (3, "new")),
        [None, "dup", None, "new"],
    ) == [1, None, 2, 3]
