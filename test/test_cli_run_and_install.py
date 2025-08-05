from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from urllib.parse import urlparse

import pytest
from typer.testing import CliRunner

from pgqueuer.cli import app


def _env_from_dsn(dsn: str) -> dict[str, str]:
    """
    Build the PG* environment variables required by asyncpg.connect()
    from a PostgreSQL DSN.
    """
    parsed = urlparse(dsn)
    # Scheme can be postgres / postgresql; path starts with /dbname
    dbname = parsed.path.lstrip("/") or ""
    host = parsed.hostname or "localhost"
    port = str(parsed.port or 5432)
    user = parsed.username or ""
    password = parsed.password or ""
    return {
        "PGHOST": host,
        "PGPORT": port,
        "PGUSER": user,
        "PGPASSWORD": password,
        "PGDATABASE": dbname,
    }


@pytest.mark.skipif(
    sys.platform.startswith("win"), reason="SIGINT process signaling differs on Windows"
)
def test_cli_run_handles_sigint_gracefully(dsn: str) -> None:
    """
    Spawn the CLI worker using the examples.consumer:main factory and ensure it
    shuts down cleanly when sent SIGINT.

    Accepts exit codes 0 (graceful) or 130 (default SIGINT termination) to avoid
    flakiness across environments while still asserting proper signal handling.
    """
    env = os.environ.copy()
    env.update(_env_from_dsn(dsn))
    # Unbuffered so logs appear promptly (mirrors CI step)
    env["PYTHONUNBUFFERED"] = "1"

    # Use -m to invoke the cli module; avoids relying on console script path.
    cmd = [
        sys.executable,
        "-m",
        "pgqueuer.cli",
        "run",
        "examples.consumer:main",
        "--dequeue-timeout",
        "1",
        "--batch-size",
        "1",
    ]

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )

    try:
        # Allow the worker to start; keep this short to not slow down test suite.
        time.sleep(2.0)
        proc.send_signal(signal.SIGINT)
        try:
            stdout, stderr = proc.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            stdout, stderr = proc.communicate()
            pytest.fail(
                f"CLI process failed to exit after SIGINT.\nSTDOUT:\n{stdout}\nSTDERR:\n{stderr}"
            )

        exit_code = proc.returncode
        # Provide rich debugging info on failure
        if exit_code not in (0, 130):
            pytest.fail(
                (
                    "Unexpected exit code "
                    f"{exit_code} (expected 0 or 130).\n"
                    f"STDOUT:\n{stdout}\n"
                    f"STDERR:\n{stderr}"
                )
            )
    finally:
        # Double safeguard in case of early exceptions
        if proc.poll() is None:
            proc.kill()


def test_cli_install_upgrade_uninstall_cycle(dsn: str) -> None:
    """
    Exercise the full install -> verify present -> upgrade -> uninstall -> verify absent flow.

    The test DB provided by the 'dsn' fixture starts with PGQueuer objects
    already installed (template cloning). To mirror the exact CI sequence
    (which starts clean), we first perform an uninstall + verify absent, then
    proceed with the normal cycle.

    Sequence:
      1. uninstall (ignore failures if already absent)
      2. verify --expect absent
      3. install
      4. verify --expect present
      5. upgrade (idempotent / no-op if latest)
      6. uninstall
      7. verify --expect absent
    """
    runner = CliRunner()

    # Build base PG* env for each CLI invocation
    base_env = os.environ.copy()
    base_env.update(_env_from_dsn(dsn))

    # Helper to invoke and assert success
    def invoke_ok(args: list[str], env: dict[str, str]) -> None:
        result = runner.invoke(app, args, env=env)
        if result.exit_code != 0:
            pytest.fail(
                "Command failed:\n"
                f"args={args}\nexit_code={result.exit_code}\n"
                f"stdout={result.stdout}\nexc={result.exception}"
            )

    # 1. Ensure absent (uninstall). If uninstall fails due to objects missing,
    #    it should still exit 0 (implementation prints SQL + runs uninstall).
    result_uninstall_initial = runner.invoke(app, ["uninstall"], env=base_env)
    # Accept exit_code 0 only.
    if result_uninstall_initial.exit_code != 0:
        pytest.fail(
            "Initial uninstall failed:\n"
            f"stdout={result_uninstall_initial.stdout}\nexc={result_uninstall_initial.exception}"
        )

    # 2. verify absent
    result_verify_absent = runner.invoke(app, ["verify", "--expect", "absent"], env=base_env)
    if result_verify_absent.exit_code != 0:
        pytest.fail(
            "verify --expect absent failed:\n"
            f"stdout={result_verify_absent.stdout}\nexc={result_verify_absent.exception}"
        )

    # 3. install
    invoke_ok(["install"], base_env)

    # 4. verify present
    invoke_ok(["verify", "--expect", "present"], base_env)

    # 5. upgrade (should succeed even if no changes)
    invoke_ok(["upgrade"], base_env)

    # 6. uninstall
    invoke_ok(["uninstall"], base_env)

    # 7. verify absent again
    invoke_ok(["verify", "--expect", "absent"], base_env)
