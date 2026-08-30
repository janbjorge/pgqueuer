"""Classify driver exceptions by their PostgreSQL SQLSTATE code.

asyncpg and psycopg both expose ``sqlstate`` on their errors, so reading it
keeps this layer free of driver imports and works for any driver that
follows the same convention.
"""

from __future__ import annotations

from pgqueuer.ports.driver import SqlStateError

UNIQUE_VIOLATION = "23505"
DEADLOCK_DETECTED = "40P01"


def code_of(exc: BaseException) -> str | None:
    """Return the SQLSTATE *exc* carries, or None if it carries none."""
    if isinstance(exc, SqlStateError) and isinstance(exc.sqlstate, str):
        return exc.sqlstate
    return None


def is_unique_violation(exc: BaseException) -> bool:
    return code_of(exc) == UNIQUE_VIOLATION


def is_deadlock_detected(exc: BaseException) -> bool:
    return code_of(exc) == DEADLOCK_DETECTED
