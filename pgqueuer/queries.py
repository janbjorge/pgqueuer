"""Backward-compatibility shim. Canonical: pgqueuer.adapters.persistence.queries"""

from pgqueuer.adapters.persistence.queries import (
    Queries,
    SyncQueries,
    is_unique_violation,
)
from pgqueuer.ports.repository import EntrypointExecutionParameter

__all__ = [
    "EntrypointExecutionParameter",
    "Queries",
    "SyncQueries",
    "is_unique_violation",
]
