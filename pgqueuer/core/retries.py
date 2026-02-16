"""
Module for retry logic and strategy management.

This module provides utilities for handling retry operations with different backoff strategies,
extracted from the TimedOverflowBuffer to separate concerns and improve maintainability.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Awaitable, Callable, Generic, TypeVar

from . import helpers, logconfig

T = TypeVar("T")


@dataclass
class RetryManager(Generic[T]):
    """
    Manages retry logic with configurable backoff strategies.

    This class handles the retry behavior for operations that may fail temporarily,
    using exponential backoff to avoid overwhelming the target system.

    Attributes:
        retry_backoff (helpers.ExponentialBackoff): Backoff strategy for regular retries.
        shutdown_backoff (helpers.ExponentialBackoff): Backoff strategy during shutdown.
        shutdown (asyncio.Event): Event that signals when retries should stop.

    """

    retry_backoff: helpers.ExponentialBackoff = field(
        default_factory=lambda: helpers.ExponentialBackoff(
            start_delay=timedelta(seconds=0.01),
            max_delay=timedelta(seconds=10),
        ),
    )
    shutdown_backoff: helpers.ExponentialBackoff = field(
        default_factory=lambda: helpers.ExponentialBackoff(
            start_delay=timedelta(milliseconds=1),
            max_delay=timedelta(milliseconds=100),
        )
    )
    shutdown: asyncio.Event = field(init=False, default_factory=asyncio.Event)

    async def execute_with_retry(
        self,
        operation: Callable[[T], Awaitable[None]],
        data: T,
        *,
        use_shutdown_backoff: bool = False,
    ) -> bool:
        """
        Execute an operation with retry logic.

        Args:
            operation: The async operation to execute.
            data: The data to pass to the operation.
            use_shutdown_backoff: Whether to use shutdown backoff strategy.

        Returns:
            bool: True if operation succeeded, False if failed after retries.
        """
        backoff = self._select_backoff(use_shutdown_backoff)

        try:
            await operation(data)
        except Exception as exc:  # noqa: BLE001 - propagate via logging
            delay = backoff.next_delay()
            logconfig.logger.warning(
                "Unable to execute %s: %s\nRetry in: %r",
                getattr(operation, "__name__", operation.__class__.__name__),
                str(exc),
                delay,
            )
            await self._sleep(delay)
            return False

        backoff.reset()
        return True

    async def retry_until_success_or_limit(
        self,
        operation: Callable[[T], Awaitable[None]],
        data: T,
        *,
        use_shutdown_backoff: bool = False,
    ) -> bool:
        """
        Retry an operation until it succeeds or reaches the backoff limit.

        Args:
            operation: The async operation to execute.
            data: The data to pass to the operation.
            use_shutdown_backoff: Whether to use shutdown backoff strategy.

        Returns:
            bool: True if operation eventually succeeded, False if limit reached.
        """
        backoff = self._select_backoff(use_shutdown_backoff)
        reached_limit = False

        while True:
            succeeded = await self.execute_with_retry(
                operation,
                data,
                use_shutdown_backoff=use_shutdown_backoff,
            )
            if succeeded:
                return True
            if self.shutdown.is_set():
                break

            if backoff.current_delay >= backoff.max_delay:
                if reached_limit:
                    break
                reached_limit = True

        return False

    def reset_backoff(self) -> None:
        """Reset the retry backoff to initial values."""
        self.retry_backoff.reset()

    def reset_shutdown_backoff(self) -> None:
        """Reset the shutdown backoff to initial values."""
        self.shutdown_backoff.reset()

    def set_shutdown(self) -> None:
        """Signal that shutdown has been requested."""
        self.shutdown.set()

    def _select_backoff(self, use_shutdown_backoff: bool) -> helpers.ExponentialBackoff:
        return self.shutdown_backoff if use_shutdown_backoff else self.retry_backoff

    async def _sleep(self, delay: timedelta) -> None:
        if self.shutdown.is_set():
            await asyncio.sleep(0)
            return

        await asyncio.sleep(helpers.timeout_with_jitter(delay).total_seconds())
