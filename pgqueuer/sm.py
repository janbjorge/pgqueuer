"""Backward-compatibility shim. Canonical: pgqueuer.core.sm"""

from pgqueuer.core.sm import SchedulerManager

__all__ = ["SchedulerManager"]
