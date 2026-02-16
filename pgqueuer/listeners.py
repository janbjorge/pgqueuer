"""Backward-compatibility shim. Canonical: pgqueuer.core.listeners"""
from pgqueuer.core.listeners import (
    EventHandler,
    EventRouter,
    HandlerTypeVar,
    PGNoticeEventListener,
    default_event_router,
    initialize_notice_event_listener,
)

__all__ = [
    "EventHandler",
    "EventRouter",
    "HandlerTypeVar",
    "PGNoticeEventListener",
    "default_event_router",
    "initialize_notice_event_listener",
]
