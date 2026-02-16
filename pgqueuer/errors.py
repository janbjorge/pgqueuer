"""Backward-compatibility shim. Canonical: pgqueuer.domain.errors"""

from pgqueuer.domain.errors import (
    DuplicateJobError,
    FailingListenerError,
    MaxRetriesExceeded,
    MaxTimeExceeded,
    PgqException,
    RetryException,
)

__all__ = [
    "DuplicateJobError",
    "FailingListenerError",
    "MaxRetriesExceeded",
    "MaxTimeExceeded",
    "PgqException",
    "RetryException",
]
