from __future__ import annotations

import os

import psycopg
import typer

from pgqueuer import PgQueuer, logconfig, types
from pgqueuer.cli import run
from pgqueuer.db import PsycopgDriver, dsn
from pgqueuer.models import Job


async def heart_beat(job: Job) -> None:
    """Example task printing a heartbeat message."""
    print("My heart goes boom boom boom with id:", job.id)


async def head_stand(job: Job) -> None:
    """Example task printing an upside-down message."""
    print("Upside down from id:", job.id)


async def pgqueuer_factory(components: list[str]) -> PgQueuer:
    connection_string = dsn(
        host=os.environ["POSTGRES_HOST"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        database=os.environ["POSTGRES_DB"],
        port=os.environ["POSTGRES_PORT"],
    )
    conn = await psycopg.AsyncConnection.connect(
        conninfo=connection_string,
        autocommit=True,
    )
    driver = PsycopgDriver(conn)
    pgq = PgQueuer(driver)

    task_queues = {
        "heart_beat": heart_beat,
        "head_stand": head_stand,
    }

    for component_name in components:
        pgq.entrypoint(component_name)(task_queues[component_name])

    return pgq


def main(
    components: str = typer.Option(
        None,
        help="Comma-separated task names to run.",
    ),
    dequeue_timeout: float = typer.Option(
        30.0,
        "--dequeue-timeout",
        help="Max seconds to wait for new jobs.",
    ),
    batch_size: int = typer.Option(
        10,
        "--batch-size",
        help="Number of jobs to pull from the queue at once.",
    ),
    restart_delay: float = typer.Option(
        5.0,
        "--restart-delay",
        help="Delay between restarts if --restart-on-failure.",
    ),
    restart_on_failure: bool = typer.Option(
        False,
        "--restart-on-failure",
        help="Restart the manager if it fails.",
    ),
    log_level: logconfig.LogLevel = typer.Option(
        logconfig.LogLevel.INFO.name,
        "--log-level",
        help="Set pgqueuer log level.",
        parser=lambda x: x.upper(),
    ),
    mode: types.QueueExecutionMode = typer.Option(
        types.QueueExecutionMode.continuous.name,
        "--mode",
        help="Queue execution mode.",
    ),
    max_concurrent_tasks: int | None = typer.Option(
        None,
        "--max-concurrent-tasks",
        help="An upper global limit for the current runner.",
    ),
    shutdown_on_listener_failure: bool = typer.Option(
        False,
        "--shutdown-on-listener-failure",
        help="Shutdown the manager if the listener fails.",
    ),
) -> None:
    components_list = components.split(",") if components else []
    run(
        factory_fn=lambda: pgqueuer_factory(components_list),  # type: ignore[arg-type]
        dequeue_timeout=dequeue_timeout,
        batch_size=batch_size,
        restart_delay=restart_delay,
        restart_on_failure=restart_on_failure,
        log_level=log_level,
        mode=mode,
        max_concurrent_tasks=max_concurrent_tasks,
        shutdown_on_listener_failure=shutdown_on_listener_failure,
    )


if __name__ == "__main__":
    typer.run(main)
