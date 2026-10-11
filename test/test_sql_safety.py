from __future__ import annotations

from pgqueuer import db


async def test_notify_round_trip(apgdriver: db.Driver) -> None:
    """driver.notify must deliver payload to a listener on the same channel."""
    import asyncio

    received = asyncio.Future[str | bytes | bytearray]()
    channel = "test_notify_safety"

    await apgdriver.add_listener(channel, received.set_result)
    await apgdriver.notify(channel, "hello")

    assert await asyncio.wait_for(received, timeout=1) == "hello"
