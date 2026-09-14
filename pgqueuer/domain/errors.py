from __future__ import annotations

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


class DuplicateJobError(PgqException):
    """Raised when enqueue violates a deduplication constraint."""

    def __init__(self, dedupe_key: list[str | None]) -> None:
        super().__init__()
        self.dedupe_key = dedupe_key


class FailingListenerError(PgqException):
    """Raised when a listener fails to process a job."""


class SchemaDriftError(PgqException):
    """Raised when an upgrade meets a schema change it cannot safely convert.

    The planner refuses rather than guessing at a cast that could lose data.
    Apply the change by hand and re-run the upgrade.
    """

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
