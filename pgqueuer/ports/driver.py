"""Port protocols for database drivers.

These Protocol classes define the contracts for database access.
"""

from __future__ import annotations

import asyncio
from typing import Any, Callable, Protocol

from typing_extensions import Self

from pgqueuer.core.tm import TaskManager


class Driver(Protocol):
    """
    Protocol defining the essential database operations for drivers.

    The `Driver` protocol specifies the methods that a database driver must implement
    to be compatible with the system. This includes methods for fetching records,
    executing queries, adding listeners for notifications, and managing the driver's lifecycle.
    """

    async def fetch(
        self,
        query: str,
        *args: Any,
    ) -> list[dict]:
        """
        Fetch multiple records from the database.

        Args:
            query (str): The SQL query to execute.
            *args (Any): Positional arguments to substitute into the query.

        Returns:
            list[dict]: A list of dictionaries representing the fetched records.
        """
        raise NotImplementedError

    async def execute(
        self,
        query: str,
        *args: Any,
    ) -> str:
        """
        Execute a SQL query and return the status message.

        Args:
            query (str): The SQL query to execute.
            *args (Any): Positional arguments to substitute into the query.

        Returns:
            str: The status message returned by the database after execution.
        """
        raise NotImplementedError

    async def add_listener(
        self,
        channel: str,
        callback: Callable[[str | bytes | bytearray], None],
    ) -> None:
        """
        Add a listener for a PostgreSQL NOTIFY channel.

        Registers a callback function to be called whenever a notification is received
        on the specified channel.

        Args:
            channel (str): The name of the PostgreSQL channel to listen on.
            callback (Callable[[str | bytes | bytearray], None]): The function to call when a
                notification is received.
        """
        raise NotImplementedError

    @property
    def shutdown(self) -> asyncio.Event:
        """
        An asyncio.Event indicating the liveness of the driver.

        This event can be used to signal when the driver is shutting down or no longer active.
        """
        raise NotImplementedError

    @property
    def tm(self) -> TaskManager:
        """
        TaskManager instance for managing background tasks.

        Provides a way to manage and await background tasks associated with the driver.
        """
        raise NotImplementedError

    async def __aenter__(self) -> Self:
        """
        Enter the runtime context related to this object.

        Returns:
            Self: Returns self to allow the driver to be used as an asynchronous context manager.
        """
        raise NotImplementedError

    async def __aexit__(self, *_: object) -> None:
        """
        Exit the runtime context and perform cleanup actions.

        Args:
            *_ (object): Ignored arguments.
        """
        raise NotImplementedError


class SyncDriver(Protocol):
    """
    Protocol defining the essential database operations for synchronous drivers.

    The `SyncDriver` protocol specifies the methods that a synchronous database driver
    must implement to be compatible with the system. This includes methods for fetching records.
    """

    def fetch(
        self,
        query: str,
        *args: Any,
    ) -> list[dict]:
        """
        Fetch multiple records from the database synchronously.

        Args:
            query (str): The SQL query to execute.
            *args (Any): Positional arguments to substitute into the query.

        Returns:
            list[dict]: A list of dictionaries representing the fetched records.
        """
        raise NotImplementedError
