"""Backward-compatibility shim. Canonical: pgqueuer.core.cache"""

from pgqueuer.core.cache import (
    UNSET,
    TTLCache,
)

__all__ = [
    "UNSET",
    "TTLCache",
]
