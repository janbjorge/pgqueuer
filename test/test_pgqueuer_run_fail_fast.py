import asyncio

import pytest

from pgqueuer.applications import PgQueuer


async def test_pgqueuer_run_propagates_scheduler_failure() -> None:
    pgq = PgQueuer.in_memory()
    queue_cancelled = asyncio.Event()

    async def queue_manager_run(**_kwargs: object) -> None:
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            queue_cancelled.set()
            raise

    async def scheduler_manager_run() -> None:
        raise RuntimeError("scheduler failure")

    pgq.qm.run = queue_manager_run  # type: ignore
    pgq.sm.run = scheduler_manager_run  # type: ignore

    with pytest.raises(RuntimeError, match="scheduler failure"):
        await pgq.run()

    assert pgq.shutdown.is_set()
    assert queue_cancelled.is_set()


async def test_pgqueuer_run_propagates_queue_failure() -> None:
    pgq = PgQueuer.in_memory()
    scheduler_cancelled = asyncio.Event()

    async def queue_manager_run(**_kwargs: object) -> None:
        raise RuntimeError("queue failure")

    async def scheduler_manager_run() -> None:
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            scheduler_cancelled.set()
            raise

    pgq.qm.run = queue_manager_run  # type: ignore
    pgq.sm.run = scheduler_manager_run  # type: ignore

    with pytest.raises(RuntimeError, match="queue failure"):
        await pgq.run()

    assert pgq.shutdown.is_set()
    assert scheduler_cancelled.is_set()


async def test_pgqueuer_run_normal_shutdown_no_exception() -> None:
    pgq = PgQueuer.in_memory()

    async def queue_manager_run(**_kwargs: object) -> None:
        await pgq.shutdown.wait()

    async def scheduler_manager_run() -> None:
        await pgq.shutdown.wait()

    pgq.qm.run = queue_manager_run  # type: ignore
    pgq.sm.run = scheduler_manager_run  # type: ignore

    # Trigger shutdown after a brief delay
    async def trigger_shutdown() -> None:
        await asyncio.sleep(0.01)
        pgq.shutdown.set()

    asyncio.create_task(trigger_shutdown())
    await pgq.run()  # Should return cleanly, no exception

    assert pgq.shutdown.is_set()


async def test_pgqueuer_run_both_fail_propagates_first() -> None:
    pgq = PgQueuer.in_memory()

    async def queue_manager_run(**_kwargs: object) -> None:
        await asyncio.sleep(0.01)
        raise RuntimeError("queue failure")

    async def scheduler_manager_run() -> None:
        await asyncio.sleep(0.01)
        raise RuntimeError("scheduler failure")

    pgq.qm.run = queue_manager_run  # type: ignore
    pgq.sm.run = scheduler_manager_run  # type: ignore

    with pytest.raises(RuntimeError, match="(queue|scheduler) failure"):
        await pgq.run()

    assert pgq.shutdown.is_set()


async def test_pgqueuer_run_cancelled_externally() -> None:
    pgq = PgQueuer.in_memory()

    async def queue_manager_run(**_kwargs: object) -> None:
        await asyncio.Future()

    async def scheduler_manager_run() -> None:
        await asyncio.Future()

    pgq.qm.run = queue_manager_run  # type: ignore
    pgq.sm.run = scheduler_manager_run  # type: ignore

    task = asyncio.create_task(pgq.run())
    await asyncio.sleep(0.01)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert pgq.shutdown.is_set()
