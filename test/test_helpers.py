from datetime import datetime, timedelta

from pgqueuer.helpers import heartbeat_buffer_timeout, perf_counter_dt


async def test_perf_counter_dt() -> None:
    assert isinstance(perf_counter_dt(), datetime)
    assert perf_counter_dt().tzinfo is not None


async def test_heartbeat_buffer_timeout() -> None:
    assert heartbeat_buffer_timeout(
        [
            timedelta(seconds=1),
        ]
    ) == timedelta(seconds=1)

    assert heartbeat_buffer_timeout(
        [
            timedelta(seconds=1),
            timedelta(seconds=2),
            timedelta(seconds=3),
        ]
    ) == timedelta(seconds=1)

    assert heartbeat_buffer_timeout([]) == timedelta(days=1)

    assert heartbeat_buffer_timeout(
        [
            timedelta(seconds=-1),
            timedelta(seconds=-2),
            timedelta(seconds=-3),
        ]
    ) == timedelta(days=1)
