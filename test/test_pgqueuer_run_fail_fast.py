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

    pgq.qm.run = queue_manager_run  # type: ignore[method-assign]
    pgq.sm.run = scheduler_manager_run  # type: ignore[method-assign]

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

    pgq.qm.run = queue_manager_run  # type: ignore[method-assign]
    pgq.sm.run = scheduler_manager_run  # type: ignore[method-assign]

    with pytest.raises(RuntimeError, match="queue failure"):
        await pgq.run()

    assert pgq.shutdown.is_set()
    assert scheduler_cancelled.is_set()
