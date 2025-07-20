import time

import pytest

from pgqueuer.buffers import RetrySettings, TimedOverflowBuffer


def native_backoff_delays(
    start: float, base: float, max_delay: float, attempts: int
) -> list[float]:
    delay = start
    delays = []
    for _ in range(attempts):
        delay = min(delay * base, max_delay)
        delays.append(delay)
    return delays


@pytest.mark.asyncio
async def test_backoff_sequence_matches_native() -> None:
    calls: list[float] = []

    async def callback(_: list[str]) -> None:
        calls.append(time.monotonic())
        if len(calls) <= 3:
            raise RuntimeError

    settings = RetrySettings(start_delay=0.02, base=2.0, max_delay=0.05, max_time=0.2)
    buf = TimedOverflowBuffer(max_size=1, callback=callback, retry_settings=settings)
    buf.events.put_nowait("x")

    await buf.flush()

    intervals = [calls[i + 1] - calls[i] for i in range(len(calls) - 1)]
    # Old implementation used `start_delay` before multiplying by the base.
    expected = native_backoff_delays(0.01, settings.base, settings.max_delay, 3)
    for actual, exp in zip(intervals, expected):
        assert actual == pytest.approx(exp, rel=0.3)
    assert len(calls) == 4


@pytest.mark.asyncio
async def test_backoff_resets_after_success() -> None:
    calls: list[float] = []
    fail = True

    async def callback(_: list[str]) -> None:
        nonlocal fail
        calls.append(time.monotonic())
        if fail:
            fail = False
            raise RuntimeError

    settings = RetrySettings(start_delay=0.02, base=2.0, max_delay=0.05, max_time=0.2)
    buf = TimedOverflowBuffer(max_size=1, callback=callback, retry_settings=settings)
    buf.events.put_nowait("a")
    await buf.flush()
    first_interval = calls[1] - calls[0]
    calls.clear()
    fail = True
    buf.events.put_nowait("b")
    await buf.flush()
    second_interval = calls[1] - calls[0]

    assert first_interval == pytest.approx(0.02, rel=0.3)
    assert second_interval == pytest.approx(0.02, rel=0.3)


@pytest.mark.asyncio
async def test_backoff_respects_max_time() -> None:
    calls = 0

    async def callback(_: list[str]) -> None:
        nonlocal calls
        calls += 1
        raise RuntimeError

    settings = RetrySettings(start_delay=0.01, base=2.0, max_delay=0.05, max_time=0.05)
    buf = TimedOverflowBuffer(max_size=1, callback=callback, retry_settings=settings)
    buf.events.put_nowait("x")

    start = time.monotonic()
    await buf.flush()
    duration = time.monotonic() - start

    assert duration >= settings.max_time
    assert calls > 1
