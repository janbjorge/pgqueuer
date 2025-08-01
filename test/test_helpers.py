from __future__ import annotations

import time
from collections.abc import Generator
from contextlib import nullcontext
from datetime import datetime, timedelta

import pytest

from pgqueuer.helpers import (
    ExponentialBackoff,
    add_schema_to_dsn,
    merge_tracing_headers,
    retry_timer_buffer_timeout,
    timeout_with_jitter,
    timer,
    utc_now,
)


async def test_perf_counter_dt() -> None:
    assert isinstance(utc_now(), datetime)
    assert utc_now().tzinfo is not None


def test_heartbeat_buffer_timeout_empty_list() -> None:
    dts = list[timedelta]()
    expected = timedelta(hours=24)
    assert retry_timer_buffer_timeout(dts) == expected


def test_heartbeat_buffer_timeout_all_dts_less_than_or_equal_to_t0() -> None:
    dts = [timedelta(seconds=-1), timedelta(seconds=0)]
    expected = timedelta(hours=24)
    assert retry_timer_buffer_timeout(dts) == expected


def test_heartbeat_buffer_timeout_positive_dts() -> None:
    dts = [timedelta(seconds=10), timedelta(seconds=5)]
    expected = timedelta(seconds=5)
    assert retry_timer_buffer_timeout(dts) == expected


def test_heartbeat_buffer_timeout_mixed_dts() -> None:
    dts = [timedelta(seconds=-5), timedelta(seconds=10)]
    expected = timedelta(seconds=10)
    assert retry_timer_buffer_timeout(dts) == expected


def test_heartbeat_buffer_timeout_custom_t0() -> None:
    dts = [timedelta(seconds=4), timedelta(seconds=6)]
    expected = timedelta(seconds=6)
    assert retry_timer_buffer_timeout(dts, _t0=timedelta(seconds=5)) == expected


def test_heartbeat_buffer_timeout_custom_default() -> None:
    dts = list[timedelta]()
    expected = timedelta(hours=48)
    assert retry_timer_buffer_timeout(dts, _default=timedelta(hours=48)) == expected


def test_delay_within_jitter_range() -> None:
    base_timeout = timedelta(seconds=10)
    jitter_span = (0.8, 1.2)

    # Call the function multiple times to check the jitter range
    for _ in range(100):
        delay = timeout_with_jitter(base_timeout, jitter_span)
        base_delay = base_timeout.total_seconds()
        assert base_delay * jitter_span[0] <= delay.total_seconds() <= base_delay * jitter_span[1]


def test_delay_is_timedelta() -> None:
    base_timeout = timedelta(seconds=5)
    delay = timeout_with_jitter(base_timeout)
    assert isinstance(delay, timedelta)


def test_custom_jitter_range() -> None:
    base_timeout = timedelta(seconds=8)
    jitter_span = (0.5, 1.5)

    # Call the function multiple times to check the custom jitter range
    for _ in range(100):
        delay = timeout_with_jitter(base_timeout, jitter_span)
        base_delay = base_timeout.total_seconds()
        assert base_delay * jitter_span[0] <= delay.total_seconds() <= base_delay * jitter_span[1]


@pytest.mark.parametrize(
    "span,raises",
    [
        ((-0.2, 0.2), True),
        ((0, 1), True),
        ((1.3, 0.5), False),
    ],
)
def test_jitter_span_validation(span: tuple[float, float], raises: bool) -> None:
    cm = pytest.raises(ValueError) if raises else nullcontext()

    with cm:
        timeout_with_jitter(timedelta(seconds=1), span)


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
    headers: list[dict | None],
    trace_headers: Generator[dict | None, None, None],
    expected: list[dict],
) -> None:
    assert merge_tracing_headers(headers, trace_headers) == expected


def test_timer() -> None:
    with timer() as elapsed:
        t1 = elapsed()
        time.sleep(0.01)
        t2 = elapsed()
        assert t2 > t1

    assert elapsed() == elapsed()

    with pytest.raises(ValueError):
        with timer() as elapsed:
            raise ValueError

        assert elapsed() == elapsed()


def test_add_schema_to_empty_dsn() -> None:
    dsn = "postgresql://user:password@host:port/dbname"
    schema = "myschema"
    expected = "postgresql://user:password@host:port/dbname?options=-csearch_path%3Dmyschema"
    assert add_schema_to_dsn(dsn, schema) == expected


def test_add_schema_to_dsn_with_existing_query() -> None:
    dsn = "postgresql://user:password@host:port/dbname?sslmode=require"
    schema = "myschema"
    expected = "postgresql://user:password@host:port/dbname?sslmode=require&options=-csearch_path%3Dmyschema"
    assert add_schema_to_dsn(dsn, schema) == expected


def test_raise_on_existing_search_path() -> None:
    dsn = "postgresql://user:password@host:port/dbname?options=-c+search_path=otherschema"
    schema = "myschema"
    with pytest.raises(ValueError, match="search_path is already set in the options parameter."):
        add_schema_to_dsn(dsn, schema)


def test_preserve_other_options_and_add_search_path() -> None:
    dsn = "postgresql://user:password@host:port/dbname?options=-cother_option=foo"
    schema = "myschema"
    expected = "postgresql://user:password@host:port/dbname?options=-cother_option%3Dfoo&options=-csearch_path%3Dmyschema"
    assert add_schema_to_dsn(dsn, schema) == expected


def test_exponential_backoff_initial_delay() -> None:
    backoff = ExponentialBackoff(
        start_delay=timedelta(1),
        multiplier=2,
        max_delay=timedelta(10),
    )
    assert backoff.current_delay == timedelta(1)


def test_exponential_backoff_next_delay() -> None:
    backoff = ExponentialBackoff(
        start_delay=timedelta(1),
        multiplier=2,
        max_delay=timedelta(10),
    )
    assert backoff.next_delay() == timedelta(2)
    assert backoff.next_delay() == timedelta(4)
    assert backoff.next_delay() == timedelta(8)
    assert backoff.next_delay() == timedelta(10)  # Capped at max_limit
    assert backoff.next_delay() == timedelta(10)  # Capped at max_limit


def test_exponential_backoff_max_limit() -> None:
    backoff = ExponentialBackoff(
        start_delay=timedelta(3),
        multiplier=3,
        max_delay=timedelta(20),
    )
    backoff.next_delay()
    assert backoff.current_delay == timedelta(9)
    backoff.next_delay()
    assert backoff.current_delay == timedelta(20)  # Capped at max_limit
    assert backoff.current_delay == timedelta(20)  # Capped at max_limit


def test_exponential_backoff_reset() -> None:
    backoff = ExponentialBackoff(
        start_delay=timedelta(5),
        multiplier=2,
        max_delay=timedelta(50),
    )
    backoff.next_delay()
    backoff.next_delay()
    assert backoff.current_delay == timedelta(20)
    backoff.reset()
    assert backoff.current_delay == timedelta(5)
