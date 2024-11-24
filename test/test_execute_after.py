from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta

from pgqueuer.db import Driver
from pgqueuer.queries import EntrypointExecutionParameter, Queries


async def test_execute_after_default_is_now(apgdriver: Driver) -> None:
    await Queries(apgdriver).enqueue("foo", None, 0, None)
    assert (
        len(
            await Queries(apgdriver).dequeue(
                10,
                {"foo": EntrypointExecutionParameter(timedelta(seconds=60), False, 0)},
                uuid.uuid4(),
            )
        )
        == 1
    )

    await Queries(apgdriver).enqueue("foo", None, 0)
    assert (
        len(
            await Queries(apgdriver).dequeue(
                10,
                {"foo": EntrypointExecutionParameter(timedelta(seconds=60), False, 0)},
                uuid.uuid4(),
            )
        )
        == 1
    )


async def test_execute_after_zero(apgdriver: Driver) -> None:
    execute_after = timedelta(seconds=0)
    await Queries(apgdriver).enqueue("foo", None, 0, execute_after)
    assert (
        len(
            await Queries(apgdriver).dequeue(
                10,
                {"foo": EntrypointExecutionParameter(timedelta(seconds=60), False, 0)},
                uuid.uuid4(),
            )
        )
        == 1
    )


async def test_execute_after_negative(apgdriver: Driver) -> None:
    execute_after = timedelta(seconds=-10)
    await Queries(apgdriver).enqueue("foo", None, 0, execute_after)
    assert (
        len(
            await Queries(apgdriver).dequeue(
                10,
                {"foo": EntrypointExecutionParameter(timedelta(seconds=60), False, 0)},
                uuid.uuid4(),
            )
        )
        == 1
    )


async def test_execute_after_1_second(apgdriver: Driver) -> None:
    execute_after = timedelta(seconds=1)
    await Queries(apgdriver).enqueue("foo", None, 0, execute_after)
    before = await Queries(apgdriver).dequeue(
        10,
        {"foo": EntrypointExecutionParameter(timedelta(seconds=60), False, 0)},
        uuid.uuid4(),
    )
    assert len(before) == 0

    await asyncio.sleep(execute_after.total_seconds())
    after = await Queries(apgdriver).dequeue(
        10,
        {"foo": EntrypointExecutionParameter(timedelta(seconds=60), False, 0)},
        uuid.uuid4(),
    )
    assert len(after) == 1


async def test_execute_after_updated_gt_execute_after(apgdriver: Driver) -> None:
    execute_after = timedelta(seconds=1)
    await Queries(apgdriver).enqueue("foo", None, 0, execute_after)
    await asyncio.sleep(execute_after.total_seconds())
    after = await Queries(apgdriver).dequeue(
        10,
        {"foo": EntrypointExecutionParameter(timedelta(seconds=60), False, 0)},
        uuid.uuid4(),
    )
    assert len(after) == 1
    assert all(x.updated > x.execute_after for x in after)
