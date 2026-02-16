"""Backward-compatibility shim. Canonical: pgqueuer.adapters.persistence.queries"""
from pgqueuer.adapters.persistence.queries import (
    EntrypointExecutionParameter,
    Queries,
    SyncQueries,
    is_unique_violation,
)

__all__ = [
    "EntrypointExecutionParameter",
    "Queries",
    "SyncQueries",
    "is_unique_violation",
]
