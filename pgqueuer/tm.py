"""Backward-compatibility shim. Canonical: pgqueuer.core.tm"""

from pgqueuer.core.tm import TaskManager

__all__ = [
    "TaskManager",
]
