from __future__ import annotations

import asyncio
import dataclasses
from datetime import datetime, timedelta
from typing import Awaitable, Callable, Generic, TypeVar, cast

T = TypeVar("T")
UNSET = object()


@dataclasses.dataclass
class TTLCache(Generic[T]):
    """
    A Time-To-Live (TTL) cache that retains a value for a specified duration and
    updates it asynchronously upon expiration.

    This cache ensures that only one update occurs at a time. If the
    cached value is expired or has never been set, all callers will await the
    asynchronous update. If the update fails, the exception will propagate to every caller.

    Attributes:
        ttl: The duration that the cached value remains valid.
        on_expired: An asynchronous callable to refresh the value when it expires.
        expires_at: The datetime when the current cached value expires.
        value: The current cached value or a sentinel (UNSET) if not yet set.
        lock: An asyncio.Lock to prevent concurrent updates.
        update_task: A reference to the currently running update task, if any.
    """

    ttl: timedelta
    on_expired: Callable[[], Awaitable[T]]
    expires_at: datetime = dataclasses.field(init=False, default_factory=datetime.now)
    value: T | object = dataclasses.field(init=False, default=UNSET)
    lock: asyncio.Lock = dataclasses.field(init=False, default_factory=asyncio.Lock)
    update_task: asyncio.Task | None = dataclasses.field(init=False, default=None)

    async def __call__(self) -> T:
        """
        Retrieve the cached value. If the cached value is present and not
        expired, it is returned immediately.

        Otherwise, an asynchronous update is triggered and all callers await
        the update. If the update fails,its exception propagates to all callers.

        Returns:
            The cached value of type T.
        """
        now = datetime.now()
        if self.value is not UNSET and now <= self.expires_at:
            return cast(T, self.value)
        if self.update_task is None or self.update_task.done():
            self.expires_at = datetime.now() + self.ttl
            self.update_task = asyncio.create_task(self._update_value())
        await self.update_task
        return cast(T, self.value)

    async def _update_value(self) -> None:
        """
        Refresh the cached value asynchronously.

        This internal method is protected by an asyncio lock to ensure that only one
        update occurs at a time. It calls the on_expired callback to retrieve a new
        value, updates the cached value, and resets the expiration time.
        Exceptions from on_expired will propagate to the caller.
        """
        async with self.lock:
            self.value = await self.on_expired()

    @classmethod
    def create(cls, ttl: timedelta, on_expired: Callable[[], Awaitable[T]]) -> TTLCache[T]:
        """
        Create a new TTLCache instance with the specified time-to-live and asynchronous callback.

        Args:
            ttl: A timedelta representing how long the cached value remains valid.
            on_expired: An asynchronous function that provides a new value when the cache expires.

        Returns:
            A TTLCache instance configured with the provided ttl and on_expired callback.
        """
        return cls(ttl=ttl, on_expired=on_expired)
