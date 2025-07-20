from typing import Any, Callable, Coroutine

import pytest
from typer.testing import CliRunner

import pgqueuer.cli as cli
from pgqueuer import factories
from pgqueuer.cli import app


def test_run_factory_envvar(monkeypatch: pytest.MonkeyPatch) -> None:
    called: dict[str, Any] = {}

    def fake_load_factory(ref: str) -> Callable[[], None]:
        called["ref"] = ref
        return lambda: None

    async def fake_runit(factory: Callable[[], None], **kwargs: Any) -> None:
        called["factory"] = factory

    monkeypatch.setattr(factories, "load_factory", fake_load_factory)
    monkeypatch.setattr(cli.supervisor, "runit", fake_runit)

    def fake_asyncio_run(coro: Coroutine[Any, Any, Any]) -> None:
        import asyncio
        asyncio.run(coro)

    monkeypatch.setattr(cli, "asyncio_run", fake_asyncio_run)

    runner = CliRunner()
    result = runner.invoke(app, ["run"], env={"PGQUEUER_FACTORY": "mod:create"})
    assert result.exit_code == 0
    assert called["ref"] == "mod:create"
    assert callable(called["factory"])

