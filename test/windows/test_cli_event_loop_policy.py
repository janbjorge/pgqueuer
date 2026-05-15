from __future__ import annotations

import asyncio
import sys
from typing import Coroutine

import pytest

from pgqueuer.adapters.cli import cli


def test_off_windows_delegates_to_inner_runner(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli.sys, "platform", "linux")

    calls: list[Coroutine[object, object, None]] = []

    def fake_run(coro: Coroutine[object, object, None]) -> None:
        calls.append(coro)
        coro.close()

    monkeypatch.setattr(cli, "_asyncio_run", fake_run)

    async def noop() -> None:
        return None

    coro = noop()
    cli.asyncio_run(coro)

    assert calls == [coro]


@pytest.mark.skipif(sys.version_info < (3, 11), reason="asyncio.Runner is 3.11+")
def test_windows_path_runs_under_selector_event_loop(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli.sys, "platform", "win32")

    captured: list[type[asyncio.AbstractEventLoop]] = []

    async def probe() -> None:
        captured.append(type(asyncio.get_running_loop()))

    cli.asyncio_run(probe())

    assert len(captured) == 1
    assert issubclass(captured[0], asyncio.SelectorEventLoop)


@pytest.mark.skipif(
    not hasattr(asyncio, "WindowsSelectorEventLoopPolicy"),
    reason="WindowsSelectorEventLoopPolicy only exists on Windows",
)
@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_python_310_path_installs_selector_policy(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli.sys, "platform", "win32")
    monkeypatch.setattr(cli.sys, "version_info", (3, 10, 0))

    seen: list[asyncio.AbstractEventLoopPolicy] = []

    def fake_run(coro: Coroutine[object, object, None]) -> None:
        seen.append(asyncio.get_event_loop_policy())
        coro.close()

    monkeypatch.setattr(cli, "_asyncio_run", fake_run)

    original_policy = asyncio.get_event_loop_policy()
    asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())
    try:

        async def noop() -> None:
            return None

        cli.asyncio_run(noop())
    finally:
        asyncio.set_event_loop_policy(original_policy)

    assert len(seen) == 1
    assert isinstance(seen[0], asyncio.WindowsSelectorEventLoopPolicy)  # type: ignore[attr-defined]
