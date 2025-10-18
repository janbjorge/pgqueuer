"""
Module for retry logic and strategy management.

This module provides utilities for handling retry operations with different backoff strategies,
extracted from the TimedOverflowBuffer to separate concerns and improve maintainability.
"""

from __future__ import annotations

import asyncio
import logging
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
        logger (logging.Logger): Logger for retry operations.
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
    logger: logging.Logger = field(default=logconfig.logger)

    async def execute_with_retry(
        self,
        operation: Callable[[T], Awaitable[None]],
        data: T,
        operation_name: str = "operation",
        use_shutdown_backoff: bool = False,
    ) -> bool:
        """
        Execute an operation with retry logic.

        Args:
            operation: The async operation to execute.
            data: The data to pass to the operation.
            operation_name: Name of the operation for logging.
            use_shutdown_backoff: Whether to use shutdown backoff strategy.

        Returns:
            bool: True if operation succeeded, False if failed after retries.
        """
        backoff = self.shutdown_backoff if use_shutdown_backoff else self.retry_backoff

        try:
            await operation(data)
            backoff.reset()
            return True
        except Exception as e:
            delay = backoff.next_delay()
            self.logger.warning(
                "Unable to execute %s: %s\nRetry in: %r",
                operation_name,
                str(e),
                delay,
            )

            if not self.shutdown.is_set():
                await asyncio.sleep(
                    helpers.timeout_with_jitter(delay).total_seconds()
                )

            return False

    async def retry_until_success_or_limit(
        self,
        operation: Callable[[T], Awaitable[None]],
        data: T,
        operation_name: str = "operation",
        use_shutdown_backoff: bool = False,
    ) -> bool:
        """
        Retry an operation until it succeeds or reaches the backoff limit.

        Args:
            operation: The async operation to execute.
            data: The data to pass to the operation.
            operation_name: Name of the operation for logging.
            use_shutdown_backoff: Whether to use shutdown backoff strategy.

        Returns:
            bool: True if operation eventually succeeded, False if limit reached.
        """
        backoff = self.shutdown_backoff if use_shutdown_backoff else self.retry_backoff

        while backoff.current_delay < backoff.max_delay:
            if await self.execute_with_retry(operation, data, operation_name, use_shutdown_backoff):
                return True

            if self.shutdown.is_set():
                break

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
