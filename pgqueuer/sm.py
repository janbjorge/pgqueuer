"""Backward-compatibility shim. Canonical: pgqueuer.core.sm"""

from __future__ import annotations

from pgqueuer.core.sm import SchedulerManager

__all__ = ["SchedulerManager"]
