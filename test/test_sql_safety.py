from __future__ import annotations

import pytest

from pgqueuer import db


async def test_notify_round_trip(apgdriver: db.Driver) -> None:
    """driver.notify must deliver payload to a listener on the same channel."""
    import asyncio

    received = asyncio.Future[str | bytes | bytearray]()
    channel = "test_notify_safety"

    await apgdriver.add_listener(channel, received.set_result)
    await apgdriver.notify(channel, "hello")

    assert await asyncio.wait_for(received, timeout=1) == "hello"


async def test_psycopg_listen_rejects_invalid_channel() -> None:
    """PsycopgDriver.add_listener must reject non-identifier channel names."""
    from unittest.mock import MagicMock

    from pgqueuer.adapters.drivers.psycopg import PsycopgDriver

    conn = MagicMock()
    conn.autocommit = True
    driver = PsycopgDriver(conn)

    with pytest.raises(ValueError, match="Invalid channel name"):
        await driver.add_listener("'; DROP TABLE --", lambda p: None)
