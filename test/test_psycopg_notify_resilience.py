from __future__ import annotations

import asyncio

import psycopg

from pgqueuer.adapters.drivers.psycopg import PsycopgDriver


async def test_notify_watcher_survives_callback_exception(dsn: str) -> None:
    """If a listener callback raises, the watcher must keep running."""
    received: list[str] = []
    call_count = 0

    def callback(payload: str | bytes | bytearray) -> None:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("deliberate test error")
        received.append(str(payload))

    channel = "test_resilience"

    async with (
        await psycopg.AsyncConnection.connect(conninfo=dsn, autocommit=True) as listener_conn,
        PsycopgDriver(listener_conn) as driver,
        await psycopg.AsyncConnection.connect(conninfo=dsn, autocommit=True) as sender_conn,
    ):
        await driver.add_listener(channel, callback)
        # Small delay to let the watcher task start.
        await asyncio.sleep(0.1)

        # First notification — callback will raise.
        await sender_conn.execute(f"NOTIFY {channel}, 'msg1'")
        await asyncio.sleep(0.5)

        # Second notification — callback should succeed if watcher survived.
        await sender_conn.execute(f"NOTIFY {channel}, 'msg2'")
        await asyncio.sleep(0.5)

    assert call_count == 2, f"Expected 2 calls, got {call_count}"
    assert received == ["msg2"], f"Expected ['msg2'], got {received}"
