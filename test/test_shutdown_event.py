from __future__ import annotations

import asyncio
import contextvars

from pgqueuer import QueueManager, shutdown
from pgqueuer.db import AsyncpgDriver


def test_get_shutdown_event_returns_same_instance() -> None:
    event = shutdown.get_shutdown_event()
    assert shutdown.get_shutdown_event() is event


def test_reset_shutdown_event_replaces_current() -> None:
    original = shutdown.get_shutdown_event()
    updated = shutdown.reset_shutdown_event()
    assert updated is not original
    assert shutdown.get_shutdown_event() is updated


def test_set_shutdown_event_overrides_current() -> None:
    custom = asyncio.Event()
    result = shutdown.set_shutdown_event(custom)
    assert result is custom
    assert shutdown.get_shutdown_event() is custom


def test_shutdown_event_is_context_local() -> None:
    primary = shutdown.reset_shutdown_event()

    def obtain_event() -> asyncio.Event:
        return shutdown.get_shutdown_event()

    other_context = contextvars.Context()
    secondary = other_context.run(obtain_event)

    assert secondary is not primary
    assert shutdown.get_shutdown_event() is primary


async def test_queue_manager_retains_initial_event(apgdriver: AsyncpgDriver) -> None:
    original = shutdown.reset_shutdown_event()
    manager = QueueManager(connection=apgdriver)

    shutdown.set_shutdown_event(asyncio.Event())

    assert manager.shutdown is original
