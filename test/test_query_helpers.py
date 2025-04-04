from __future__ import annotations

from datetime import timedelta

from pgqueuer.query_helpers import NormedEnqueueParam, normalize_enqueue_params


def test_normalize_single_entrypoint():
    result = normalize_enqueue_params(
        entrypoint="task1", payload=b"data1", priority=1, execute_after=timedelta(seconds=10)
    )
    expected = NormedEnqueueParam(
        priority=[1],
        entrypoint=["task1"],
        payload=[b"data1"],
        execute_after=[timedelta(seconds=10)],
    )
    assert result == expected


def test_normalize_multiple_entrypoints():
    result = normalize_enqueue_params(
        entrypoint=["task1", "task2"],
        payload=[b"data1", b"data2"],
        priority=[1, 2],
        execute_after=[timedelta(seconds=10), timedelta(seconds=20)],
    )
    expected = NormedEnqueueParam(
        priority=[1, 2],
        entrypoint=["task1", "task2"],
        payload=[b"data1", b"data2"],
        execute_after=[timedelta(seconds=10), timedelta(seconds=20)],
    )
    assert result == expected


def test_normalize_single_entrypoint_no_execute_after():
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
    )
    assert result == expected


def test_normalize_multiple_entrypoints_no_execute_after():
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
    )
    assert result == expected


def test_normalize_mixed_execute_after():
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
    )
    assert result == expected


def test_normalize_single_entrypoint_none_payload():
    result = normalize_enqueue_params(
        entrypoint="task1",
        payload=None,
        priority=1,
        execute_after=timedelta(seconds=10),
    )
    expected = NormedEnqueueParam(
        priority=[1],
        entrypoint=["task1"],
        payload=[None],
        execute_after=[timedelta(seconds=10)],
    )
    assert result == expected


def test_normalize_multiple_entrypoints_none_payload():
    result = normalize_enqueue_params(
        entrypoint=["task1", "task2"],
        payload=[None, b"data2"],
        priority=[1, 2],
        execute_after=[timedelta(seconds=10), timedelta(seconds=20)],
    )
    expected = NormedEnqueueParam(
        priority=[1, 2],
        entrypoint=["task1", "task2"],
        payload=[None, b"data2"],
        execute_after=[timedelta(seconds=10), timedelta(seconds=20)],
    )
    assert result == expected
