import asyncio

import uvloop


async def test_uvloop() -> None:
    loop = asyncio.get_event_loop()
    assert isinstance(loop, uvloop.Loop), "uvloop is not being used"
