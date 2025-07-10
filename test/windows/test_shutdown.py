import asyncio
from datetime import timedelta
from typing import cast

import pytest

from pgqueuer import qm, supervisor, types


class DummyManager:
    def __init__(self) -> None:
        self.shutdown = asyncio.Event()

    async def run(self, *_: object, **__: object) -> None:
        await self.shutdown.wait()


async def test_shutdown_without_signal(monkeypatch: pytest.MonkeyPatch) -> None:
    loop = asyncio.get_running_loop()

    def boom(*_: object) -> None:
        raise NotImplementedError

    monkeypatch.setattr(loop, "add_signal_handler", boom)

    shutdown_event = asyncio.Event()
    dummy = DummyManager()

    async def factory() -> qm.QueueManager:
        return cast(qm.QueueManager, dummy)

    async def run_manager(manager: DummyManager, *args: object, **kwargs: object) -> None:
        await manager.run()

    def setup_shutdown_handlers_stub(
        manager: DummyManager, shutdown: asyncio.Event
    ) -> DummyManager:
        manager.shutdown = shutdown
        return manager

    monkeypatch.setattr(supervisor, "run_manager", run_manager)
    monkeypatch.setattr(supervisor, "setup_shutdown_handlers", setup_shutdown_handlers_stub)

    task = asyncio.create_task(
        supervisor.runit(
            factory,
            dequeue_timeout=timedelta(seconds=1),
            batch_size=1,
            restart_delay=timedelta(seconds=0),
            restart_on_failure=False,
            shutdown=shutdown_event,
            mode=types.QueueExecutionMode.continuous,
            max_concurrent_tasks=None,
            shutdown_on_listener_failure=False,
        )
    )

    await asyncio.sleep(0.1)
    shutdown_event.set()
    await task
