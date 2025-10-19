"""
Context-local shutdown signal management.

Components share a common `asyncio.Event` stored in a ContextVar so each logical
execution context can coordinate graceful termination without relying on a
process-scoped global.
"""

from __future__ import annotations

import asyncio
import contextvars
from typing import Final

SHUTDOWN_EVENT: Final[contextvars.ContextVar[asyncio.Event | None]] = contextvars.ContextVar(
    "pgqueuer_shutdown_event",
    default=None,
)


def get_shutdown_event() -> asyncio.Event:
    """Return the current shutdown event, creating one for this context if absent."""
    event = SHUTDOWN_EVENT.get()
    if event is None:
        event = asyncio.Event()
        SHUTDOWN_EVENT.set(event)
    return event


def set_shutdown_event(event: asyncio.Event) -> asyncio.Event:
    """Associate *event* with the current context and return it."""
    SHUTDOWN_EVENT.set(event)
    return event


def reset_shutdown_event() -> asyncio.Event:
    """Set and return a fresh shutdown event for the current context."""
    event = asyncio.Event()
    SHUTDOWN_EVENT.set(event)
    return event
