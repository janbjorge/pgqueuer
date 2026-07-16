"""Facade for the web dashboard adapter (requires the ``web`` extra)."""

from __future__ import annotations

from pgqueuer.adapters.web.app import create_web_app
from pgqueuer.adapters.web.routes import create_web_router

__all__ = [
    "create_web_app",
    "create_web_router",
]
