from __future__ import annotations

import pytest
import uvicorn
from typer.testing import CliRunner

from pgqueuer.adapters.cli.cli import app


@pytest.fixture
def uvicorn_calls(monkeypatch: pytest.MonkeyPatch) -> list[dict]:
    calls: list[dict] = []
    monkeypatch.setattr(
        uvicorn,
        "run",
        lambda _asgi, host, port: calls.append({"host": host, "port": port}),
    )
    return calls


def test_cli_web_defaults(uvicorn_calls: list[dict]) -> None:
    result = CliRunner().invoke(app, ["web"])
    assert result.exit_code == 0, result.output
    assert uvicorn_calls == [{"host": "127.0.0.1", "port": 8080}]


def test_cli_web_flags(uvicorn_calls: list[dict]) -> None:
    result = CliRunner().invoke(app, ["web", "--host", "0.0.0.0", "--port", "9000"])
    assert result.exit_code == 0, result.output
    assert uvicorn_calls == [{"host": "0.0.0.0", "port": 9000}]


def test_cli_web_env_vars(uvicorn_calls: list[dict]) -> None:
    result = CliRunner().invoke(
        app,
        ["web"],
        env={"PGQUEUER_WEB_HOST": "0.0.0.0", "PGQUEUER_WEB_PORT": "9001"},
    )
    assert result.exit_code == 0, result.output
    assert uvicorn_calls == [{"host": "0.0.0.0", "port": 9001}]


def test_cli_web_flag_beats_env(uvicorn_calls: list[dict]) -> None:
    result = CliRunner().invoke(
        app,
        ["web", "--port", "9002"],
        env={"PGQUEUER_WEB_PORT": "9999"},
    )
    assert result.exit_code == 0, result.output
    assert uvicorn_calls == [{"host": "127.0.0.1", "port": 9002}]
