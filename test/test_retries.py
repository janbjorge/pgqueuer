import asyncio
from datetime import timedelta

import pytest

from pgqueuer import helpers, retries


@pytest.mark.asyncio
async def test_retry_manager_execute_success_resets_backoff() -> None:
    backoff = helpers.ExponentialBackoff(
        start_delay=timedelta(milliseconds=1),
        max_delay=timedelta(milliseconds=8),
    )
    backoff.next_delay()  # advance state away from start_delay
    manager: retries.RetryManager[list[int]] = retries.RetryManager(
        retry_backoff=backoff,
    )

    async def noop(_: list[int]) -> None:
        await asyncio.sleep(0)

    succeeded = await manager.execute_with_retry(noop, [1, 2, 3])

    assert succeeded is True
    assert backoff.current_delay == backoff.start_delay


@pytest.mark.asyncio
async def test_retry_manager_execute_failure_invokes_backoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backoff = helpers.ExponentialBackoff(
        start_delay=timedelta(milliseconds=1),
        max_delay=timedelta(milliseconds=8),
    )
    manager: retries.RetryManager[list[int]] = retries.RetryManager(
        retry_backoff=backoff,
    )

    sleep_calls: list[float] = []
    jitter_calls: list[timedelta] = []

    async def fake_sleep(delay: float) -> None:
        sleep_calls.append(delay)

    def fake_jitter(delay: timedelta) -> timedelta:
        jitter_calls.append(delay)
        return timedelta(milliseconds=2)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(helpers, "timeout_with_jitter", fake_jitter)

    async def fail(_: list[int]) -> None:
        raise RuntimeError("boom")

    succeeded = await manager.execute_with_retry(fail, [1])

    assert succeeded is False
    assert backoff.current_delay == timedelta(milliseconds=2)
    assert jitter_calls == [timedelta(milliseconds=2)]
    assert sleep_calls == [pytest.approx(timedelta(milliseconds=2).total_seconds())]


@pytest.mark.asyncio
async def test_retry_manager_retry_until_success_or_limit() -> None:
    backoff = helpers.ExponentialBackoff(
        start_delay=timedelta(milliseconds=1),
        max_delay=timedelta(milliseconds=4),
    )
    manager: retries.RetryManager[list[int]] = retries.RetryManager(
        retry_backoff=backoff,
    )

    attempts = 0

    async def op(_: list[int]) -> None:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise RuntimeError("try again")

    succeeded = await manager.retry_until_success_or_limit(op, [1])

    assert succeeded is True
    assert attempts == 3


@pytest.mark.asyncio
async def test_retry_manager_stops_when_shutdown_set() -> None:
    backoff = helpers.ExponentialBackoff(
        start_delay=timedelta(milliseconds=1),
        max_delay=timedelta(milliseconds=2),
    )
    manager: retries.RetryManager[list[int]] = retries.RetryManager(
        retry_backoff=backoff,
    )
    manager.set_shutdown()

    async def fail(_: list[int]) -> None:
        raise RuntimeError("fail")

    succeeded = await manager.retry_until_success_or_limit(fail, [1])

    assert succeeded is False
