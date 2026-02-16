"""Backward-compatibility shim. Canonical: pgqueuer.core.retries"""

from pgqueuer.core.retries import RetryManager

__all__ = [
    "RetryManager",
]
