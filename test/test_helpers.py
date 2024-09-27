from datetime import datetime, timedelta

from pgqueuer.helpers import perf_counter_dt, retry_timer_buffer_timeout


async def test_perf_counter_dt() -> None:
    assert isinstance(perf_counter_dt(), datetime)
    assert perf_counter_dt().tzinfo is not None


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
