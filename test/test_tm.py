import asyncio

import pytest
from PgQueuer import qm


@pytest.mark.parametrize("N", (1, 2, 64, 1024))
async def test_task_manager(N: int) -> None:
    future = asyncio.Future[None]()

    tm = qm.TaskManager()
    assert len(tm.tasks) == 0

    async def waiter(future: asyncio.Future) -> None:
        return await future

    for _ in range(N):
        tm.add(asyncio.create_task(waiter(future)))

    assert len(tm.tasks) == N

    future.set_result(None)
    await asyncio.gather(*tm.tasks)
    assert len(tm.tasks) == 0
