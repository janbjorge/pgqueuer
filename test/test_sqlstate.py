from __future__ import annotations

import asyncpg
import psycopg
import pytest

from pgqueuer.adapters.persistence import sqlstate
from pgqueuer.adapters.persistence.queries import lost_capacity_slot_race


@pytest.mark.parametrize(
    "exc, expected",
    (
        (asyncpg.UniqueViolationError(), sqlstate.UNIQUE_VIOLATION),
        (asyncpg.DeadlockDetectedError(), sqlstate.DEADLOCK_DETECTED),
        (psycopg.errors.UniqueViolation(), sqlstate.UNIQUE_VIOLATION),
        (psycopg.errors.DeadlockDetected(), sqlstate.DEADLOCK_DETECTED),
    ),
)
def test_code_of_reads_the_sqlstate_both_drivers_carry(
    exc: Exception,
    expected: str,
) -> None:
    assert sqlstate.code_of(exc) == expected


def test_code_of_returns_none_for_an_exception_without_a_sqlstate() -> None:
    assert sqlstate.code_of(ValueError("not from the database")) is None


def test_code_of_ignores_a_sqlstate_that_is_not_a_string() -> None:
    # asyncpg builds errors from server fields; a driver that leaves the
    # attribute unset must not be read as carrying a code.
    class Unset(Exception):
        sqlstate = None

    assert sqlstate.code_of(Unset()) is None


@pytest.mark.parametrize(
    "exc, lost",
    (
        (asyncpg.UniqueViolationError(), True),
        (asyncpg.DeadlockDetectedError(), True),
        (psycopg.errors.UniqueViolation(), True),
        (psycopg.errors.DeadlockDetected(), True),
        (asyncpg.SerializationError(), False),
        (psycopg.errors.NotNullViolation(), False),
        (ValueError("not from the database"), False),
    ),
)
def test_lost_capacity_slot_race_matches_only_the_slot_race(
    exc: Exception,
    lost: bool,
) -> None:
    assert lost_capacity_slot_race(exc) is lost
