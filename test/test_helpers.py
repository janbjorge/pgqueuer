from __future__ import annotations

from collections.abc import Generator, Mapping

import pytest

from pgqueuer.adapters.persistence.query_helpers import merge_tracing_headers


@pytest.mark.parametrize(
    "headers, trace_headers, expected",
    [
        (
            [{"key1": "value1"}],
            [{"trace_key": "trace_value"}],
            [{"key1": "value1", "trace_key": "trace_value"}],
        ),
        (
            [None],
            [{"trace_key": "trace_value"}],
            [{"trace_key": "trace_value"}],
        ),
        (
            [{"key1": "value1"}],
            [None],
            [{"key1": "value1"}],
        ),
        (
            [None],
            [None],
            [{}],
        ),
    ],
)
def test_merge_tracing_headers(
    headers: list[Mapping[str, object] | None],
    trace_headers: Generator[Mapping[str, object] | None, None, None],
    expected: list[Mapping[str, object] | None],
) -> None:
    assert merge_tracing_headers(headers, trace_headers) == expected
