"""Backward-compatibility shim. Canonical: pgqueuer.core.heartbeat"""
from pgqueuer.core.heartbeat import Heartbeat

__all__ = [
    "Heartbeat",
]
