"""Backward-compatibility shim. Canonical: pgqueuer.domain.errors"""

from __future__ import annotations

from pgqueuer.domain.errors import (
    DuplicateJobError,
    FailingListenerError,
    PgqException,
    RetryException,
    RetryRequested,
)

__all__ = [
    "DuplicateJobError",
    "FailingListenerError",
    "PgqException",
    "RetryException",
    "RetryRequested",
]
