from __future__ import annotations

import uuid
from datetime import timedelta

import asyncpg
import psycopg
import pytest

from pgqueuer.adapters.persistence import sqlstate
from pgqueuer.adapters.persistence.queries import Queries, lost_capacity_slot_race
from pgqueuer.domain.settings import DBSettings
from pgqueuer.domain.types import QueueEntrypoint, QueueManagerId
from pgqueuer.models import Job
from pgqueuer.queries import EntrypointExecutionParameter

SLOT_INDEX = f"{DBSettings().queue_table}_picked_slot_idx"
OTHER_INDEX = f"{DBSettings().queue_table}_unique_dedupe_key"


class AsyncpgNamedUnique(asyncpg.UniqueViolationError):
    def __init__(self, constraint_name: str | None) -> None:
        super().__init__()
        self.constraint_name = constraint_name


class _Diag:
    def __init__(self, constraint_name: str | None) -> None:
        self.constraint_name = constraint_name


class PsycopgNamedUnique(Exception):
    """psycopg exposes the constraint on ``diag``, which is not settable on the real error."""

    sqlstate = sqlstate.UNIQUE_VIOLATION

    def __init__(self, constraint_name: str | None) -> None:
        super().__init__()
        self.diag = _Diag(constraint_name)


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
    "exc, expected",
    (
        (AsyncpgNamedUnique(SLOT_INDEX), SLOT_INDEX),
        (PsycopgNamedUnique(SLOT_INDEX), SLOT_INDEX),
        (AsyncpgNamedUnique(None), None),
        (PsycopgNamedUnique(None), None),
        (asyncpg.UniqueViolationError(), None),
        (ValueError("not from the database"), None),
    ),
)
def test_constraint_of_reads_the_name_both_drivers_carry(
    exc: Exception,
    expected: str | None,
) -> None:
    assert sqlstate.constraint_of(exc) == expected


@pytest.mark.parametrize(
    "exc, lost",
    (
        (AsyncpgNamedUnique(SLOT_INDEX), True),
        (PsycopgNamedUnique(SLOT_INDEX), True),
        (asyncpg.DeadlockDetectedError(), True),
        (psycopg.errors.DeadlockDetected(), True),
        (AsyncpgNamedUnique(OTHER_INDEX), False),
        (PsycopgNamedUnique(OTHER_INDEX), False),
        (asyncpg.UniqueViolationError(), False),
        (psycopg.errors.UniqueViolation(), False),
        (asyncpg.SerializationError(), False),
        (psycopg.errors.NotNullViolation(), False),
        (ValueError("not from the database"), False),
    ),
)
def test_lost_capacity_slot_race_matches_only_the_slot_index(
    exc: Exception,
    lost: bool,
) -> None:
    assert lost_capacity_slot_race(exc, SLOT_INDEX) is lost


class _FetchBoom:
    def __init__(self, exc: Exception) -> None:
        self.exc = exc

    async def fetch(self, query: str, *args: object) -> list[dict[str, object]]:
        raise self.exc


async def _dequeue(driver: object) -> list[Job]:
    return await Queries(driver).dequeue(  # type: ignore[arg-type]
        batch_size=1,
        entrypoints={QueueEntrypoint("fetch"): EntrypointExecutionParameter(1)},
        queue_manager_id=QueueManagerId(uuid.uuid4()),
        global_concurrency_limit=None,
        heartbeat_timeout=timedelta(seconds=30),
    )


async def test_dequeue_returns_empty_on_slot_unique_violation() -> None:
    assert await _dequeue(_FetchBoom(AsyncpgNamedUnique(SLOT_INDEX))) == []
    assert await _dequeue(_FetchBoom(PsycopgNamedUnique(SLOT_INDEX))) == []
    assert await _dequeue(_FetchBoom(asyncpg.DeadlockDetectedError())) == []


async def test_dequeue_reraises_unrelated_unique_violation() -> None:
    with pytest.raises(asyncpg.UniqueViolationError):
        await _dequeue(_FetchBoom(AsyncpgNamedUnique(OTHER_INDEX)))
    with pytest.raises(PsycopgNamedUnique):
        await _dequeue(_FetchBoom(PsycopgNamedUnique(OTHER_INDEX)))
    with pytest.raises(asyncpg.UniqueViolationError):
        await _dequeue(_FetchBoom(asyncpg.UniqueViolationError()))
