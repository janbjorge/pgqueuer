from __future__ import annotations

from typing import Any

import pytest
from typer.testing import CliRunner

from pgqueuer.adapters.cli import cli, supervisor
from pgqueuer.core import logconfig
from pgqueuer.domain import types


@pytest.mark.parametrize("flag", [[], ["--log-level", "debug"], ["--log-level", "WARNING"]])
def test_run_passes_enums_not_strings(monkeypatch: pytest.MonkeyPatch, flag: list[str]) -> None:
    """Regression for #1097: --log-level reached setup_fancy_logger as a str."""
    seen: dict[str, Any] = {}

    async def fake_run(_factory: object, **kwargs: Any) -> None:
        seen.update(kwargs)

    monkeypatch.setattr(logconfig, "setup_fancy_logger", lambda level: seen.update(level=level))
    monkeypatch.setattr(supervisor, "run", fake_run)

    result = CliRunner().invoke(cli.app, ["run", "examples.consumer:main", *flag])

    assert result.exit_code == 0, result.output
    assert isinstance(seen["level"], logconfig.LogLevel)
    assert isinstance(seen["mode"], types.QueueExecutionMode)
