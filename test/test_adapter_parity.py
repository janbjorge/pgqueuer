"""Compare one repository script on the in-memory and PostgreSQL adapters.

Cases that already agree run as normal tests. Each known disagreement is its
own test, marked ``xfail(strict=True)``, so a fix turns that test green and
leaves the others in place.

Wall-clock fields are left out. Schema DDL, NOTIFY payloads, and heartbeat
takeover are left out. ``Job.slot`` is left out: PostgreSQL seats a limited
entrypoint, and the in-memory adapter enforces that limit by counting.
Statistic rows are summed per entrypoint, priority, and status.
"""

from __future__ import annotations

import uuid
from collections.abc import Awaitable
from datetime import timedelta
from typing import Literal, assert_never

import pytest

from pgqueuer.adapters.inmemory import InMemoryDriver, InMemoryQueries
from pgqueuer.adapters.persistence import queries, sqlstate
from pgqueuer.db import AsyncpgDriver
from pgqueuer.domain.errors import DuplicateJobError
from pgqueuer.domain.models import (
    CronExpressionEntrypoint,
    Job,
    Log,
    LogStatistics,
    QueueStatistics,
    Schedule,
    ThroughputStats,
)
from pgqueuer.domain.types import (
    CronEntrypoint,
    CronExpression,
    JobId,
    QueueEntrypoint,
    QueueManagerId,
)
from pgqueuer.ports.repository import EntrypointExecutionParameter

Repository = queries.Queries | InMemoryQueries

OWNER = QueueManagerId(uuid.UUID("00000000-0000-0000-0000-000000000001"))
TIMEOUT = timedelta(seconds=30)
EP = {QueueEntrypoint("ep"): EntrypointExecutionParameter(0)}


def pair(apgdriver: AsyncpgDriver) -> tuple[InMemoryQueries, queries.Queries]:
    return InMemoryQueries(driver=InMemoryDriver()), queries.Queries(apgdriver)


def classify(exc: BaseException) -> tuple[str, str | None]:
    if isinstance(exc, DuplicateJobError):
        return ("DuplicateJobError", None)
    code = sqlstate.code_of(exc)
    if code is not None:
        return (code, sqlstate.constraint_of(exc))
    return (f"{type(exc).__module__}.{type(exc).__name__}", None)


def job_tuple(job: Job) -> tuple[object, ...]:
    return (
        int(job.id),
        job.status,
        str(job.entrypoint),
        job.priority,
        job.attempts,
        job.payload,
        None if job.queue_manager_id is None else str(job.queue_manager_id),
        job.headers,
    )


def log_sequences(logs: list[Log]) -> list[tuple[int, tuple[str, ...]]]:
    ordered = sorted(logs, key=lambda log: (log.created, int(log.job_id), log.status))
    groups: dict[int, list[str]] = {}
    for log in ordered:
        groups.setdefault(int(log.job_id), []).append(log.status)
    return sorted((job_id, tuple(statuses)) for job_id, statuses in groups.items())


def norm_list(value: list[object]) -> object:
    if not value:
        return []
    head = value[0]
    if isinstance(head, LogStatistics):
        totals: dict[tuple[str, int, str], int] = {}
        for item in value:
            if not isinstance(item, LogStatistics):
                return [norm_value(entry) for entry in value]
            key = (str(item.entrypoint), item.priority, item.status)
            totals[key] = totals.get(key, 0) + item.count
        return sorted((*key, count) for key, count in totals.items())
    if isinstance(head, ThroughputStats):
        rows: list[tuple[str, str, int]] = []
        for item in value:
            if not isinstance(item, ThroughputStats):
                return [norm_value(entry) for entry in value]
            rows.append((str(item.entrypoint), item.status, item.total_count))
        return sorted(rows)
    return [norm_value(item) for item in value]


def norm_value(value: object) -> object:
    if isinstance(value, Job):
        return job_tuple(value)
    if isinstance(value, QueueStatistics):
        return (value.count, str(value.entrypoint), value.priority, value.status)
    if isinstance(value, Schedule):
        return (int(value.id), str(value.expression), str(value.entrypoint), value.status)
    if isinstance(value, list):
        return norm_list(value)
    return value


async def snapshot(q: Repository) -> dict[str, object]:
    jobs = await q.browse_queue(limit=500)
    logs = await q.queue_log()
    failed = await q.list_failed_jobs(limit=500)
    size = norm_value(await q.queue_size())
    assert isinstance(size, list)
    exceptions = await q.exception_logs(limit=500)
    schedules = await q.peek_schedule()
    ages = await q.queue_age()
    durations = await q.job_duration_percentiles(timedelta(hours=1))
    return {
        "jobs": [job_tuple(job) for job in jobs],
        "logs": log_sequences(logs),
        "failed": [job_tuple(job) for job in failed],
        "size": sorted(size, key=str),
        "unaggregated": await q.unaggregated_log_count(),
        "exceptions": log_sequences(exceptions),
        "schedules": norm_value(schedules),
        "ages": sorted((str(row.entrypoint), row.queued_count) for row in ages),
        "durations": sorted((str(row.entrypoint), row.completed) for row in durations),
    }


def as_jobs(value: object) -> list[Job]:
    if not isinstance(value, list) or not all(isinstance(item, Job) for item in value):
        raise AssertionError(value)
    return value


def as_id(value: object) -> JobId:
    if not isinstance(value, list) or len(value) != 1 or not isinstance(value[0], int):
        raise AssertionError(value)
    return JobId(value[0])


async def trace(q: Repository) -> list[object]:
    found: list[object] = []

    async def step(name: str, call: Awaitable[object]) -> object:
        try:
            value = await call
        except DuplicateJobError as exc:
            found.append((name, "err", classify(exc)))
            value = None
        else:
            found.append((name, "ok", norm_value(value)))
        found.append((f"{name}.state", await snapshot(q)))
        return value

    await step("enqueue-high", q.enqueue("ep", b"high", priority=5))
    await step("enqueue-low", q.enqueue("ep", b"low", priority=1))
    await step("enqueue-other", q.enqueue("other", b"side"))
    await step("defer", q.enqueue("ep", b"later", execute_after=timedelta(hours=1)))
    picked = as_jobs(await step("dequeue", q.dequeue(10, EP, OWNER, None, TIMEOUT)))
    await step("complete", q.log_jobs([(job, "successful", None) for job in picked]))

    await step("enqueue-g1", q.enqueue("g", b"g1"))
    await step("enqueue-g2", q.enqueue("g", b"g2"))
    global_ep = {QueueEntrypoint("g"): EntrypointExecutionParameter(0)}
    first = as_jobs(await step("dequeue-global", q.dequeue(10, global_ep, OWNER, 1, TIMEOUT)))
    await step("global-blocked", q.dequeue(10, global_ep, OWNER, 1, TIMEOUT))
    await step("complete-global", q.log_jobs([(first[0], "exception", None)]))

    plain = as_id(await step("enqueue-plain", q.enqueue("plain", b"x")))
    plain_ep = {QueueEntrypoint("plain"): EntrypointExecutionParameter(0)}
    (failed_job,) = as_jobs(await step("pick-plain", q.dequeue(1, plain_ep, OWNER, None, TIMEOUT)))
    await step("fail-plain", q.log_jobs([(failed_job, "failed", None)]))
    await step("requeue-plain", q.requeue_jobs([plain]))

    hold = {QueueEntrypoint("hold"): EntrypointExecutionParameter(0)}
    await step("enqueue-hold", q.enqueue("hold", b"hold", dedupe_key="hold"))
    (held,) = as_jobs(await step("pick-hold", q.dequeue(1, hold, OWNER, None, TIMEOUT)))
    await step("fail-hold", q.log_jobs([(held, "failed", None)]))
    await step("enqueue-hold-again", q.enqueue("hold", b"hold-2", dedupe_key="hold"))

    cancel = {QueueEntrypoint("cancel"): EntrypointExecutionParameter(0)}
    cancel_id = as_id(
        await step("enqueue-cancel", q.enqueue("cancel", b"cancel", dedupe_key="cancel"))
    )
    await step("pick-cancel", q.dequeue(1, cancel, OWNER, None, TIMEOUT))
    await step("cancel", q.mark_job_as_cancelled([cancel_id]))
    await step("enqueue-cancel-again", q.enqueue("cancel", b"cancel-2", dedupe_key="cancel"))

    retry = {QueueEntrypoint("retry"): EntrypointExecutionParameter(0)}
    await step("enqueue-retry", q.enqueue("retry", b"retry", dedupe_key="retry"))
    (retried,) = as_jobs(await step("pick-retry", q.dequeue(1, retry, OWNER, None, TIMEOUT)))
    await step("retry", q.retry_job(retried, timedelta(0), None))

    await step("clear-other", q.clear_queue("other"))
    await step("stats", q.log_statistics(limit=None))
    await step("throughput", q.throughput_summary())
    await step("missing", q.queue_job_by_id(JobId(10**9)))

    schedule = CronExpressionEntrypoint(CronEntrypoint("hourly"), CronExpression("0 * * * *"))
    await step("schedule", q.insert_schedule({schedule: timedelta(0)}))
    await step("schedule-again", q.insert_schedule({schedule: timedelta(0)}))
    fetched = await step("fetch-schedule", q.fetch_schedule({schedule: timedelta(minutes=5)}))
    if isinstance(fetched, list) and fetched and isinstance(fetched[0], Schedule):
        await step("schedule-queued", q.set_schedule_queued({fetched[0].id}))
    await step("delete-schedule", q.delete_schedule(set(), {CronEntrypoint("hourly")}))

    await step("duplicate-hold", q.enqueue("hold", b"hold-3", dedupe_key="hold"))
    await step("duplicate-retry", q.enqueue("retry", b"retry-2", dedupe_key="retry"))
    return found


async def test_repository_trace_matches(apgdriver: AsyncpgDriver) -> None:
    """In-memory and PostgreSQL agree on the script, apart from the xfail cases."""
    memory, postgres = pair(apgdriver)
    left = await trace(memory)
    right = await trace(postgres)
    assert len(left) == len(right)
    for index, (mem, pg) in enumerate(zip(left, right, strict=True)):
        if mem != pg:
            raise AssertionError(f"{index}\nMEM {mem!r}\nPG  {pg!r}")


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="in-memory queued_work counts rows; PostgreSQL returns 0 or 1",
)
async def test_queued_work_matches(apgdriver: AsyncpgDriver) -> None:
    memory, postgres = pair(apgdriver)
    eps = [QueueEntrypoint("ep")]
    for q in (memory, postgres):
        await q.enqueue("ep", None, execute_after=timedelta(hours=1))
        await q.enqueue("ep", None)
    assert await memory.queued_work(eps) == await postgres.queued_work(eps)
    assert await memory.eligible_queued_work(eps) == await postgres.eligible_queued_work(eps)


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="in-memory queue_size sorts by entrypoint; PostgreSQL sorts by count, then enum order",
)
async def test_queue_size_order_matches(apgdriver: AsyncpgDriver) -> None:
    memory, postgres = pair(apgdriver)
    entrypoints = {QueueEntrypoint("a"): EntrypointExecutionParameter(0)}
    for q in (memory, postgres):
        await q.enqueue("b", b"1")
        await q.enqueue(["a", "a"], [b"1", b"2"], [0, 0])
        await q.dequeue(1, entrypoints, OWNER, None, TIMEOUT)
    assert norm_value(await memory.queue_size()) == norm_value(await postgres.queue_size())


Conflict = Literal["raise", "skip", "update_priority"]


async def id_gap(q: Repository, conflict: Conflict) -> int:
    first = as_id(await q.enqueue("ep", b"a", priority=1, dedupe_key="k"))
    if conflict == "raise":
        with pytest.raises(DuplicateJobError):
            await q.enqueue("ep", b"b", dedupe_key="k")
    elif conflict == "skip":
        assert await q.enqueue("ep", b"b", dedupe_key="k", on_conflict="skip") == [None]
    elif conflict == "update_priority":
        bumped = await q.enqueue(
            "ep",
            b"b",
            priority=5,
            dedupe_key="k",
            on_conflict="update_priority",
        )
        assert bumped == [first]
    else:
        assert_never(conflict)
    return int(as_id(await q.enqueue("ep", b"c", dedupe_key="next"))) - int(first)


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="in-memory does not consume a job id on a dedupe skip, update, or raise",
)
@pytest.mark.parametrize("conflict", ["raise", "skip", "update_priority"])
async def test_dedupe_conflict_consumes_job_id(
    apgdriver: AsyncpgDriver, conflict: Conflict
) -> None:
    memory, postgres = pair(apgdriver)
    assert await id_gap(memory, conflict) == await id_gap(postgres, conflict)


async def test_requeue_keeps_dedupe_key(apgdriver: AsyncpgDriver) -> None:
    memory, postgres = pair(apgdriver)

    async def run(q: Repository) -> tuple[str, list[tuple[str, int]]]:
        job_id = as_id(await q.enqueue("ep", b"first", dedupe_key="sync-42"))
        (job,) = await q.dequeue(1, EP, OWNER, None, TIMEOUT)
        await q.log_jobs([(job, "failed", None)])
        await q.requeue_jobs([job_id])
        try:
            await q.enqueue("ep", b"second", dedupe_key="sync-42")
        except DuplicateJobError:
            outcome = "duplicate"
        else:
            outcome = "accepted"
        size = [(row.status, row.count) for row in await q.queue_size()]
        return outcome, sorted(size)

    assert await run(memory) == await run(postgres)


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="in-memory requeue ignores a dedupe key a queued job already holds",
)
async def test_requeue_conflict_matches(apgdriver: AsyncpgDriver) -> None:
    memory, postgres = pair(apgdriver)

    async def requeue_error(q: Repository, ids: list[JobId]) -> tuple[str, str | None] | None:
        try:
            await q.requeue_jobs(ids)
        except Exception as exc:
            return classify(exc)
        return None

    async def run(q: Repository) -> tuple[object, list[tuple[str, str]]]:
        original = as_id(await q.enqueue("ep", b"a", dedupe_key="k"))
        (job,) = await q.dequeue(1, EP, OWNER, None, TIMEOUT)
        await q.log_jobs([(job, "failed", None)])
        await q.enqueue("ep", b"b", dedupe_key="k")
        single = await requeue_error(q, [original])

        batch = {
            QueueEntrypoint("ba"): EntrypointExecutionParameter(0),
            QueueEntrypoint("bb"): EntrypointExecutionParameter(0),
        }
        ba = as_id(await q.enqueue("ba", b"ba", dedupe_key="ba"))
        bb = as_id(await q.enqueue("bb", b"bb", dedupe_key="bb"))
        picked = await q.dequeue(2, batch, OWNER, None, TIMEOUT)
        await q.log_jobs([(row, "failed", None) for row in picked])
        await q.enqueue("bb", b"live", dedupe_key="bb")
        jobs = [(str(row.entrypoint), row.status) for row in await q.browse_queue(limit=20)]
        return (single, await requeue_error(q, [ba, bb])), sorted(jobs)

    assert await run(memory) == await run(postgres)


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="in-memory peek_schedule keeps insert order; PostgreSQL orders by last_run",
)
async def test_peek_schedule_order_matches(apgdriver: AsyncpgDriver) -> None:
    memory, postgres = pair(apgdriver)

    async def run(q: Repository) -> list[str]:
        hourly = CronExpression("0 * * * *")
        await q.insert_schedule(
            {CronExpressionEntrypoint(CronEntrypoint("a"), hourly): timedelta(0)}
        )
        await q.insert_schedule(
            {CronExpressionEntrypoint(CronEntrypoint("b"), hourly): timedelta(0)}
        )
        rows = {str(row.entrypoint): row.id for row in await q.peek_schedule()}
        await q.set_schedule_queued({rows["b"]})
        await q.set_schedule_queued({rows["a"]})
        return [str(row.entrypoint) for row in await q.peek_schedule()]

    assert await run(memory) == await run(postgres)


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="in-memory job_status follows log order; PostgreSQL orders by job_id",
)
async def test_job_status_order_matches(apgdriver: AsyncpgDriver) -> None:
    memory, postgres = pair(apgdriver)

    async def run(q: Repository) -> list[tuple[int, str]]:
        first = as_id(await q.enqueue("ep", b"a"))
        second = as_id(await q.enqueue("ep", b"b"))
        rows = await q.job_status([second, first])
        return [(int(job_id), status) for job_id, status in rows]

    assert await run(memory) == await run(postgres)


async def test_throughput_summary_order_matches(apgdriver: AsyncpgDriver) -> None:
    memory, postgres = pair(apgdriver)

    async def run(q: Repository) -> list[tuple[str, int]]:
        await q.enqueue("ep", b"a")
        (job,) = await q.dequeue(1, EP, OWNER, None, TIMEOUT)
        await q.log_jobs([(job, "successful", None)])
        rows = await q.throughput_summary()
        return [(row.status, row.total_count) for row in rows]

    assert await run(memory) == await run(postgres)
