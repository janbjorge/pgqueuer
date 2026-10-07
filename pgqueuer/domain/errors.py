from __future__ import annotations

import sys
from datetime import timedelta

from pgqueuer.domain.types import ColumnName, SqlType, TableName


class PgqException(Exception):
    """Base class for all exceptions raised by PgQueuer."""


class RetryException(PgqException):
    """Exception raised for retry-related errors in PgQueuer."""


class RetryRequested(RetryException):
    """Raise inside a job handler to request a database-level retry.

    Instead of marking the job as a terminal failure, the job is re-queued
    with status 'queued', a bumped execute_after, and incremented attempts.

    Attributes:
        delay: Time to wait before the next attempt.
        reason: Optional human-readable explanation for the retry.
    """

    def __init__(
        self,
        delay: timedelta = timedelta(0),
        reason: str | None = None,
    ) -> None:
        super().__init__(reason or "Retry requested")
        self.delay = delay
        self.reason = reason


def retry_request(exc: BaseException) -> RetryRequested | None:
    """Return the retry *exc* asks for, or ``None``.

    That is *exc* itself, or, for an exception group made only of retries (such as
    a TaskGroup whose children raised RetryRequested), the one with the longest delay.
    """
    if isinstance(exc, RetryRequested):
        return exc
    if sys.version_info >= (3, 11):
        if isinstance(exc, BaseExceptionGroup):  # noqa: F821 -- builtin from 3.11, gated above
            found = [retry_request(inner) for inner in exc.exceptions]
            retries = [retry for retry in found if retry is not None]
            if retries and len(retries) == len(found):
                return max(retries, key=lambda retry: retry.delay)
    return None


class DuplicateJobError(PgqException):
    """Raised when enqueue violates a deduplication constraint."""

    def __init__(self, dedupe_key: list[str | None]) -> None:
        super().__init__()
        self.dedupe_key = dedupe_key


class FailingListenerError(PgqException):
    """Raised when a listener fails to process a job."""


class SchemaDriftError(PgqException):
    """Raised when an upgrade meets a schema change it cannot safely convert."""

    def __init__(
        self,
        *,
        table: TableName,
        column: ColumnName,
        installed: SqlType,
        declared: SqlType,
    ) -> None:
        super().__init__(
            f"{table}.{column} is {installed} in the database but declared {declared}; "
            "no supported conversion. Alter the column by hand, then re-run upgrade."
        )
        self.table = table
        self.column = column
        self.installed = installed
        self.declared = declared
