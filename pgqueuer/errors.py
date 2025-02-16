from __future__ import annotations

from datetime import datetime


class PgqException(Exception):
    """Base class for all exceptions raised by PGQueuer."""


class BaseRetryException(PgqException):
    """Exception raised for retry-related errors in PGQueuer."""


class RetryableException(BaseRetryException):
    """Exception raised for retry-related errors in PGQueuer."""

    def __init__(self, schedule_for: datetime | None):
        self.schedule_for = schedule_for


class MaxRetriesExceeded(BaseRetryException):
    """Exception raised when all retry attempts have been exhausted."""


class MaxTimeExceeded(BaseRetryException):
    """Exception raised when the maximum time limit for retries has been exceeded."""
