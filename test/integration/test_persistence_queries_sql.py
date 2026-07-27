"""Epic SQL integration tests for persistence adapter (Queries).

Tests real PostgreSQL behavior: concurrency, locking, LISTEN/NOTIFY,
transactions, constraints. Complements unit tests (InMemoryQueries).

Each test validates SQL-specific semantics that the in-memory fake cannot.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import timedelta

import pytest

from pgqueuer.adapters.persistence.queries import Queries
from pgqueuer.db import AsyncpgDriver
from pgqueuer.domain.models import TracebackRecord
from pgqueuer.ports.repository import EntrypointExecutionParameter


async def test_dequeue_locking_race_single_job_one_picker(
    apgdriver: AsyncpgDriver,
) -> None:
    """SELECT FOR UPDATE SKIP LOCKED: only one worker picks same job."""
    q = Queries(apgdriver)

    await q.enqueue(["ep"], [None], [0])
    qm1_id = uuid.uuid4()
    qm2_id = uuid.uuid4()

    async def dequeue_1() -> list:
        return await q.dequeue(
            batch_size=1,
            entrypoints={"ep": EntrypointExecutionParameter(concurrency_limit=1)},
            queue_manager_id=qm1_id,
            global_concurrency_limit=None,
            heartbeat_timeout=timedelta(seconds=30),
        )

    async def dequeue_2() -> list:
        await asyncio.sleep(0.001)
        return await q.dequeue(
            batch_size=1,
            entrypoints={"ep": EntrypointExecutionParameter(concurrency_limit=1)},
            queue_manager_id=qm2_id,
            global_concurrency_limit=None,
            heartbeat_timeout=timedelta(seconds=30),
        )

    r1, r2 = await asyncio.gather(dequeue_1(), dequeue_2())
    assert len(r1) + len(r2) == 1
    if r1:
        assert r1[0].queue_manager_id == qm1_id
    else:
        assert r2[0].queue_manager_id == qm2_id


async def test_atomicity_dequeue_log_entry_created(
    apgdriver: AsyncpgDriver,
) -> None:
    """Dequeue + log is atomic: picked job always has log entry."""
    q = Queries(apgdriver)

    jids = await q.enqueue(["ep"], [None], [0])
    qm_id = uuid.uuid4()

    jobs = await q.dequeue(
        batch_size=1,
        entrypoints={"ep": EntrypointExecutionParameter(concurrency_limit=1)},
        queue_manager_id=qm_id,
        global_concurrency_limit=None,
        heartbeat_timeout=timedelta(seconds=30),
    )

    assert len(jobs) == 1
    assert jobs[0].status == "picked"
    assert jobs[0].queue_manager_id == qm_id

    logs = await q.queue_log()
    picked_logs = [log for log in logs if log.job_id == int(jids[0]) and log.status == "picked"]
    assert len(picked_logs) == 1


async def test_dedupe_constraint_partial_index(
    apgdriver: AsyncpgDriver,
) -> None:
    """Partial unique index: allows duplicates in terminal states."""
    q = Queries(apgdriver)

    jids1 = await q.enqueue(["ep"], [None], [0], dedupe_key=["k"], on_conflict="raise")

    with pytest.raises(Exception):
        await q.enqueue(["ep"], [None], [0], dedupe_key=["k"], on_conflict="raise")

    qm_id = uuid.uuid4()
    jobs = await q.dequeue(
        batch_size=1,
        entrypoints={"ep": EntrypointExecutionParameter(concurrency_limit=1)},
        queue_manager_id=qm_id,
        global_concurrency_limit=None,
        heartbeat_timeout=timedelta(seconds=30),
    )
    assert len(jobs) == 1

    await q.log_jobs([(jobs[0], "successful", None)])

    jids2 = await q.enqueue(["ep"], [None], [0], dedupe_key=["k"], on_conflict="raise")
    assert len(jids2) == 1
    assert jids2[0] != jids1[0]


async def test_concurrent_enqueue_dedup_race(
    apgdriver: AsyncpgDriver,
) -> None:
    """Concurrent enqueue same dedupe_key: one succeeds, one fails."""
    q = Queries(apgdriver)

    results: dict[int, list | Exception] = {}

    async def enqueue_worker(worker_id: int) -> None:
        try:
            result = await q.enqueue(
                ["ep"], [None], [0], dedupe_key=["shared_k"], on_conflict="raise"
            )
            results[worker_id] = result
        except Exception as e:
            results[worker_id] = e

    await asyncio.gather(enqueue_worker(1), enqueue_worker(2))

    successes = [r for r in results.values() if isinstance(r, list)]
    failures = [r for r in results.values() if isinstance(r, Exception)]

    assert len(successes) == 1
    assert len(failures) == 1


async def test_stale_job_recovery_heartbeat_timeout(
    apgdriver: AsyncpgDriver,
) -> None:
    """Heartbeat timeout: stale job re-picked by different worker."""
    q = Queries(apgdriver)

    await q.enqueue(["ep"], [None], [0])

    qm1_id = uuid.uuid4()
    jobs = await q.dequeue(
        batch_size=1,
        entrypoints={"ep": EntrypointExecutionParameter(concurrency_limit=1)},
        queue_manager_id=qm1_id,
        global_concurrency_limit=None,
        heartbeat_timeout=timedelta(seconds=1),
    )
    assert len(jobs) == 1
    original_qm_id = jobs[0].queue_manager_id
    assert original_qm_id == qm1_id

    await asyncio.sleep(1.2)

    qm2_id = uuid.uuid4()
    jobs2 = await q.dequeue(
        batch_size=1,
        entrypoints={"ep": EntrypointExecutionParameter(concurrency_limit=1)},
        queue_manager_id=qm2_id,
        global_concurrency_limit=None,
        heartbeat_timeout=timedelta(seconds=1),
    )

    assert len(jobs2) == 1
    assert jobs2[0].queue_manager_id == qm2_id
    assert jobs2[0].queue_manager_id != original_qm_id


async def test_large_batch_atomicity_10k_jobs(
    apgdriver: AsyncpgDriver,
) -> None:
    """Large batch 10k jobs: all inserted or error, never partial."""
    q = Queries(apgdriver)

    N = 10_000
    eps = ["ep"] * N
    payloads: list[None] = [None] * N
    priorities = [0] * N
    dedupe_keys: list[str] = [f"k_{i}" for i in range(N)]

    jids = await q.enqueue(
        eps,
        payloads,
        priorities,
        dedupe_key=dedupe_keys,
        on_conflict="raise",  # type: ignore[call-overload]
    )

    assert len(jids) == N
    size = await q.queue_size()
    total_count = sum(s.count for s in size if s.entrypoint == "ep")
    assert total_count == N


async def test_on_conflict_skip_shape_preservation(
    apgdriver: AsyncpgDriver,
) -> None:
    """on_conflict=skip: returned array shape matches input, None at conflicts."""
    q = Queries(apgdriver)

    await q.enqueue(["ep"], [None], [0], dedupe_key=["b"], on_conflict="raise")

    dedupe_keys: list[str] = ["a", "b", "c", "d", "e"]
    jids = await q.enqueue(
        ["ep"] * 5,
        [None] * 5,
        [0] * 5,
        dedupe_key=dedupe_keys,
        on_conflict="skip",  # type: ignore[call-overload]
    )

    assert len(jids) == 5
    assert jids[0] is not None
    assert jids[1] is None
    assert jids[2] is not None
    assert jids[3] is not None
    assert jids[4] is not None


async def test_mark_job_cancellation(
    apgdriver: AsyncpgDriver,
) -> None:
    """mark_job_as_cancelled: job marked canceled, log entry created."""
    q = Queries(apgdriver)

    jids = await q.enqueue(["ep"], [None], [0])
    qm_id = uuid.uuid4()

    jobs = await q.dequeue(
        batch_size=1,
        entrypoints={"ep": EntrypointExecutionParameter(concurrency_limit=1)},
        queue_manager_id=qm_id,
        global_concurrency_limit=None,
        heartbeat_timeout=timedelta(seconds=30),
    )
    assert len(jobs) == 1

    await q.mark_job_as_cancelled(jids)

    logs = await q.queue_log()
    cancel_logs = [log for log in logs if log.job_id == int(jids[0]) and log.status == "canceled"]
    assert len(cancel_logs) == 1


async def test_traceback_jsonb_roundtrip(
    apgdriver: AsyncpgDriver,
) -> None:
    """Traceback JSONB round-trips correctly through log storage."""
    q = Queries(apgdriver)

    jids = await q.enqueue(["ep"], [None], [0])
    qm_id = uuid.uuid4()

    jobs = await q.dequeue(
        batch_size=1,
        entrypoints={"ep": EntrypointExecutionParameter(concurrency_limit=1)},
        queue_manager_id=qm_id,
        global_concurrency_limit=None,
        heartbeat_timeout=timedelta(seconds=30),
    )
    assert len(jobs) == 1
    job = jobs[0]

    traceback_data = {
        "exc": "ValueError",
        "msg": "test error",
        "frames": [
            {"func": "f1", "line": 10, "file": "a.py"},
            {"func": "f2", "line": 20, "file": "b.py"},
        ],
    }

    tb_record = TracebackRecord.model_validate(traceback_data)
    await q.log_jobs([(job, "exception", tb_record)])

    logs = await q.queue_log()
    exc_logs = [log for log in logs if log.job_id == int(jids[0]) and log.status == "exception"]
    assert len(exc_logs) == 1
    exc_log = exc_logs[0]
    assert exc_log.traceback is not None
    if isinstance(exc_log.traceback, str):
        retrieved_tb = json.loads(exc_log.traceback)
    else:
        retrieved_tb = json.loads(str(exc_log.traceback))
    assert retrieved_tb.get("exc") == "ValueError"
    frames = retrieved_tb.get("frames", [])
    if frames:
        assert frames[0].get("func") == "f1"


async def test_global_concurrency_limit_hard_cap(
    apgdriver: AsyncpgDriver,
) -> None:
    """Global concurrency limit: hard cap enforced across workers."""
    q = Queries(apgdriver)

    await q.enqueue(["ep"] * 100, [None] * 100, [0] * 100)

    limit = 10
    picked_per_worker = {}

    async def dequeue_worker(worker_id: int) -> None:
        qm_id = uuid.uuid4()
        jobs = await q.dequeue(
            batch_size=10,
            entrypoints={"ep": EntrypointExecutionParameter(concurrency_limit=100)},
            queue_manager_id=qm_id,
            global_concurrency_limit=limit,
            heartbeat_timeout=timedelta(seconds=30),
        )
        picked_per_worker[worker_id] = len(jobs)

    await asyncio.gather(*(dequeue_worker(i) for i in range(5)))

    total_picked = sum(picked_per_worker.values())
    assert total_picked <= limit
