import asyncio

import anyio
import pytest


@pytest.mark.anyio
async def test_task_group_runs_tasks() -> None:
    results: list[int] = []

    async def worker(x: int) -> None:
        await asyncio.sleep(0.01)
        results.append(x)

    async with anyio.create_task_group() as tg:
        for i in range(5):
            tg.start_soon(worker, i)

    assert sorted(results) == list(range(5))


@pytest.mark.anyio
async def test_task_group_propagates_exceptions() -> None:
    async def boom() -> None:
        raise ValueError("fail")

    with pytest.raises(ValueError):
        async with anyio.create_task_group() as tg:
            tg.start_soon(boom)
