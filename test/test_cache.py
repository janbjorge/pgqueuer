from __future__ import annotations

import asyncio
from datetime import datetime, timedelta

import pytest
import time_machine

from pgqueuer.cache import TTLCache


async def test_initial_fetch() -> None:
    count = [0]

    async def on_expired() -> str:
        count[0] += 1
        return "value"

    cache = TTLCache(
        ttl=timedelta(seconds=5),
        on_expired=on_expired,
    )
    result = await cache()
    assert result == "value"
    assert count[0] == 1


async def test_stale_value() -> None:
    count = [0]

    async def on_expired() -> str:
        count[0] += 1
        return "cached_value"

    cache = TTLCache(
        ttl=timedelta(seconds=5),
        on_expired=on_expired,
    )
    assert await cache() == await cache()
    assert count[0] == 1


async def test_expiration() -> None:
    call_count = [0]

    async def on_expired() -> str:
        call_count[0] += 1
        return f"value_{call_count[0]}"

    cache = TTLCache(
        ttl=timedelta(seconds=2),
        on_expired=on_expired,
    )
    assert await cache() == "value_1"
    with time_machine.travel(datetime.now() + timedelta(seconds=3)):
        assert await cache() == await cache() == "value_2"


async def test_concurrent_access() -> None:
    call_count = [0]

    async def on_expired() -> str:
        await asyncio.sleep(0.01)
        call_count[0] += 1
        return f"concurrent_{call_count[0]}"

    cache = TTLCache(
        ttl=timedelta(seconds=5),
        on_expired=on_expired,
    )
    tasks = [asyncio.create_task(cache()) for _ in range(10)]
    results = await asyncio.gather(*tasks)
    assert all(result == results[0] for result in results)
    assert call_count[0] == 1


async def test_create_method() -> None:
    async def on_expired() -> int:
        return 42

    cache = TTLCache.create(
        ttl=timedelta(seconds=1),
        on_expired=on_expired,
    )
    assert await cache() == 42


async def test_cache_exception_propagation() -> None:
    class CustomException(Exception):
        pass

    async def on_expired() -> str:
        raise CustomException

    cache = TTLCache(
        ttl=timedelta(seconds=1),
        on_expired=on_expired,
    )

    with pytest.raises(CustomException):
        await cache()

    t1, t2 = await asyncio.gather(cache(), cache(), return_exceptions=True)

    assert isinstance(t1, CustomException)
    assert isinstance(t2, CustomException)


async def test_expiration_resets_after_update() -> None:
    """Cache TTL should be counted from when the update finishes."""

    call_count = 0

    async def on_expired() -> str:
        nonlocal call_count
        call_count += 1
        await asyncio.sleep(0.05)
        return f"value_{call_count}"

    ttl = timedelta(seconds=0.2)
    cache = TTLCache(ttl=ttl, on_expired=on_expired)

    start = datetime.now()
    value1 = await cache()
    expires_after_first = cache.expires_at

    # Expiration should be at least `ttl` seconds after update finished
    assert expires_after_first - start >= ttl

    # Call again shortly before expiration; value should still be cached
    await asyncio.sleep(0.18)
    assert await cache() == value1
    assert call_count == 1

    # After expiration we should refresh and update the expiration timestamp
    await asyncio.sleep(0.1)
    value2 = await cache()
    assert call_count == 2
    assert value2 != value1
    assert cache.expires_at > expires_after_first
