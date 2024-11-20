from __future__ import annotations


class PgqException(Exception):
    """Base class for all exceptions raised by PGQueuer."""


class RetryException(PgqException):
    """Exception raised for retry-related errors in PGQueuer."""


class MaxRetriesExceeded(RetryException):
    """Exception raised when all retry attempts have been exhausted."""


class MaxTimeExceeded(RetryException):
    """Exception raised when the maximum time limit for retries has been exceeded."""
