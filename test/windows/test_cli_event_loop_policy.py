from __future__ import annotations

import asyncio
import sys
from collections.abc import Coroutine

import pytest

from pgqueuer.adapters.cli import cli


def test_off_windows_delegates_to_asyncio_run(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setattr(cli, "HAS_UVLOOP", False)

    calls: list[Coroutine[object, object, None]] = []

    def fake_run(coro: Coroutine[object, object, None]) -> None:
        calls.append(coro)
        coro.close()

    monkeypatch.setattr(asyncio, "run", fake_run)

    async def noop() -> None:
        return None

    coro = noop()
    cli.asyncio_run(coro)

    assert calls == [coro]


def test_windows_path_runs_under_selector_event_loop(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "platform", "win32")

    captured: list[type[asyncio.AbstractEventLoop]] = []

    async def probe() -> None:
        captured.append(type(asyncio.get_running_loop()))

    cli.asyncio_run(probe())

    assert len(captured) == 1
    assert issubclass(captured[0], asyncio.SelectorEventLoop)
