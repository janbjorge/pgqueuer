from __future__ import annotations

import asyncio
import dataclasses
from datetime import datetime, timedelta
from typing import Awaitable, Callable, Generic, TypeVar, cast

T = TypeVar("T")
UNSET = object()


@dataclasses.dataclass
class TTLCache(Generic[T]):
    """Single-flight TTL cache: concurrent callers share one in-flight refresh."""

    ttl: timedelta
    on_expired: Callable[[], Awaitable[T]]
    expires_at: datetime = dataclasses.field(init=False, default_factory=datetime.now)
    value: T | object = dataclasses.field(init=False, default=UNSET)
    lock: asyncio.Lock = dataclasses.field(init=False, default_factory=asyncio.Lock)
    update_task: asyncio.Task | None = dataclasses.field(init=False, default=None)

    async def __call__(self) -> T:
        """Return the cached value, refreshing once if expired/unset."""
        now = datetime.now()
        if self.value is not UNSET and now <= self.expires_at:
            return cast(T, self.value)
        if self.update_task is None or self.update_task.done():
            self.update_task = asyncio.create_task(self._update_value())
        await self.update_task
        return cast(T, self.value)

    async def _update_value(self) -> None:
        async with self.lock:
            self.value = await self.on_expired()
            self.expires_at = datetime.now() + self.ttl

    @classmethod
    def create(cls, ttl: timedelta, on_expired: Callable[[], Awaitable[T]]) -> TTLCache[T]:
        return cls(ttl=ttl, on_expired=on_expired)
