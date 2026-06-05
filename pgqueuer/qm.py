"""Backward-compatibility shim. Canonical: pgqueuer.core.qm"""

from __future__ import annotations

from pgqueuer.core.qm import QueueManager

__all__ = ["QueueManager"]
