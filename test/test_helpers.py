from __future__ import annotations

import time
from datetime import datetime, timedelta

import pytest

from pgqueuer.helpers import (
    add_schema_to_dsn,
    normalize_cron_expression,
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
    delay_multiplier = 2.0
    jitter_span = (0.8, 1.2)

    # Call the function multiple times to check the jitter range
    for _ in range(100):
        delay = timeout_with_jitter(base_timeout, delay_multiplier, jitter_span)
        base_delay = base_timeout.total_seconds() * delay_multiplier
        assert base_delay * jitter_span[0] <= delay.total_seconds() <= base_delay * jitter_span[1]


def test_delay_is_timedelta() -> None:
    base_timeout = timedelta(seconds=5)
    delay_multiplier = 1.5
    delay = timeout_with_jitter(base_timeout, delay_multiplier)
    assert isinstance(delay, timedelta)


def test_custom_jitter_range() -> None:
    base_timeout = timedelta(seconds=8)
    delay_multiplier = 1.0
    jitter_span = (0.5, 1.5)

    # Call the function multiple times to check the custom jitter range
    for _ in range(100):
        delay = timeout_with_jitter(base_timeout, delay_multiplier, jitter_span)
        base_delay = base_timeout.total_seconds() * delay_multiplier
        assert base_delay * jitter_span[0] <= delay.total_seconds() <= base_delay * jitter_span[1]


@pytest.mark.parametrize(
    "expression, expected",
    (
        ("@hourly", "0 * * * *"),
        ("@midnight", "0 0 * * *"),
    ),
)
def test_normalize_cron_expression(expression: str, expected: str) -> None:
    assert normalize_cron_expression(expression) == expected


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
    expected = "postgresql://user:password@host:port/dbname?options=-c+search_path%3Dmyschema"
    assert add_schema_to_dsn(dsn, schema) == expected


def test_add_schema_to_dsn_with_existing_query() -> None:
    dsn = "postgresql://user:password@host:port/dbname?sslmode=require"
    schema = "myschema"
    expected = "postgresql://user:password@host:port/dbname?sslmode=require&options=-c+search_path%3Dmyschema"
    assert add_schema_to_dsn(dsn, schema) == expected


def test_raise_on_existing_search_path() -> None:
    dsn = "postgresql://user:password@host:port/dbname?options=-c+search_path=otherschema"
    schema = "myschema"
    with pytest.raises(ValueError, match="search_path is already set in the options parameter."):
        add_schema_to_dsn(dsn, schema)


def test_preserve_other_options_and_add_search_path() -> None:
    dsn = "postgresql://user:password@host:port/dbname?options=-c+other_option=foo"
    schema = "myschema"
    expected = "postgresql://user:password@host:port/dbname?options=-c+other_option%3Dfoo&options=-c+search_path%3Dmyschema"
    assert add_schema_to_dsn(dsn, schema) == expected
