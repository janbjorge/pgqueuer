"""Backward-compatibility shim. Canonical: pgqueuer.core.qm"""
from pgqueuer.core.qm import QueueManager

__all__ = ["QueueManager"]
