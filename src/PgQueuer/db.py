import asyncio
from typing import Any, Callable, Protocol, Sequence

import asyncpg


class Driver(Protocol):
    async def fetch(
        self,
        query: str,
        *args: Any,
    ) -> list[Any]: ...
    async def execute(
        self,
        query: str,
        *args: Any,
    ) -> str: ...
    async def executemany(
        self,
        query: str,
        *args: Sequence,
    ) -> None: ...
    async def add_listener(
        self,
        channal: str,
        callback: Callable[[str | bytes | bytearray], None],
    ) -> None: ...
    async def fetchval(
        self,
        query: str,
        *args: Any,
    ) -> Any: ...


class AsyncPGDriver(Driver):
    def __init__(self, connection: asyncpg.Connection) -> None:
        super().__init__()
        self.lock = asyncio.Lock()
        self.connection = connection

    async def fetch(
        self,
        query: str,
        *args: Any,
    ) -> list:
        async with self.lock:
            return await self.connection.fetch(query, *args)

    async def execute(
        self,
        query: str,
        *args: Any,
    ) -> str:
        async with self.lock:
            return await self.connection.execute(query, *args)

    async def executemany(
        self,
        query: str,
        *args: Sequence,
    ) -> None:
        async with self.lock:
            return await self.connection.executemany(query, *args)

    async def fetchval(self, query: str, *args: Any) -> Any:
        async with self.lock:
            return await self.connection.fetchval(query, *args)

    async def add_listener(
        self,
        channal: str,
        callback: Callable[[str | bytes | bytearray], None],
    ) -> None:
        async with self.lock:
            await self.connection.add_listener(
                channal,
                lambda x: callback(x[-1]),
            )
