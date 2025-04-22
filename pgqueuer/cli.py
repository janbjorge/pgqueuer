from __future__ import annotations

import asyncio
import contextlib
import os
from dataclasses import dataclass
from datetime import timedelta
from typing import Awaitable, Callable

import typer
from tabulate import tabulate
from typer import Context
from typing_extensions import AsyncGenerator

from . import db, factories, helpers, listeners, logconfig, models, qb, queries, supervisor, types

try:
    from uvloop import run as asyncio_run
except ImportError:
    from asyncio import run as asyncio_run  # type: ignore[assignment]

app = typer.Typer(
    help=(
        "PgQueuer CLI: Manage and monitor recurring schedules with PostgreSQL, "
        "featuring dashboards, real-time tracking, and schema tools."
    ),
    epilog="Explore documentation and examples: https://github.com/janbjorge/pgqueuer",
    no_args_is_help=True,
    pretty_exceptions_show_locals=False,
)


@dataclass
class AppConfig:
    """Application configuration for PGQueuer CLI."""

    prefix: str = ""
    pg_dsn: str = ""
    pg_host: str = ""
    pg_port: str = "5432"
    pg_user: str = ""
    pg_database: str = ""
    pg_password: str = ""
    pg_schema: str = ""
    factory_fn_ref: str | None = None

    def setup_env(self) -> None:
        if self.prefix:
            os.environ["PGQUEUER_PREFIX"] = self.prefix

    @property
    def dsn(self) -> str:
        dsn = self.pg_dsn or db.dsn(
            database=self.pg_database,
            password=self.pg_password,
            port=self.pg_port,
            user=self.pg_user,
            host=self.pg_host,
        )
        if self.pg_schema is not None:
            dsn = helpers.add_schema_to_dsn(dsn, self.pg_schema)
        return dsn


@app.callback()
def main(
    ctx: Context,
    prefix: str = typer.Option(
        "",
        help="Prefix for pgqueuer objects.",
        envvar="PGQUEUER_PREFIX",
    ),
    pg_dsn: str = typer.Option(
        None,
        help="Database DSN.",
        envvar="PGDSN",
    ),
    pg_host: str = typer.Option(
        None,
        help="Database host.",
        envvar="PGHOST",
    ),
    pg_port: str = typer.Option(
        "5432",
        help="Database port.",
        envvar="PGPORT",
    ),
    pg_user: str = typer.Option(
        None,
        help="Database user.",
        envvar="PGUSER",
    ),
    pg_database: str = typer.Option(
        None,
        help="Database name.",
        envvar="PGDATABASE",
    ),
    pg_password: str = typer.Option(
        None,
        help="Database password.",
        envvar="PGPASSWORD",
    ),
    pg_schema: str = typer.Option(
        None,
        help="Database schema.",
        envvar="PGSCHEMA",
    ),
    factory_fn_ref: str | None = typer.Option(
        None,
        "--factory",
        help="A reference to a function that returns an instance of Queries",
    ),
) -> None:
    """Main Typer callback to set up shared configuration."""
    config = AppConfig(
        prefix=prefix,
        pg_dsn=pg_dsn,
        pg_host=pg_host,
        pg_port=pg_port,
        pg_user=pg_user,
        pg_database=pg_database,
        pg_password=pg_password,
        pg_schema=pg_schema,
        factory_fn_ref=factory_fn_ref,
    )
    config.setup_env()
    ctx.obj = config


def create_default_queries_factory(
    config: AppConfig,
    settings: qb.DBSettings,
) -> Callable[..., Awaitable[queries.Queries]]:
    """
    This is the default implementation of a factory that returns an instance of Queries.
    It attempts asyncpg first, then psycopg.
    """

    async def factory() -> queries.Queries:
        with contextlib.suppress(ImportError):
            import asyncpg

            return queries.Queries(
                db.AsyncpgDriver(await asyncpg.connect(dsn=config.dsn)),
                qbe=qb.QueryBuilderEnvironment(settings),
                qbq=qb.QueryQueueBuilder(settings),
                qbs=qb.QuerySchedulerBuilder(settings),
            )
        with contextlib.suppress(ImportError):
            import psycopg

            return queries.Queries(
                db.PsycopgDriver(
                    await psycopg.AsyncConnection.connect(config.dsn, autocommit=True)
                ),
                qbe=qb.QueryBuilderEnvironment(settings),
                qbq=qb.QueryQueueBuilder(settings),
                qbs=qb.QuerySchedulerBuilder(settings),
            )
        raise RuntimeError("Neither asyncpg nor psycopg could be imported.")

    return factory


@contextlib.asynccontextmanager
async def yield_queries(
    ctx: Context,
    settings: qb.DBSettings,
) -> AsyncGenerator[queries.Queries, None]:
    """
    Async context manager that yields a Queries instance from either a user-supplied
    factory function or the default factory.
    """
    config: AppConfig = ctx.obj
    if config.factory_fn_ref:
        factory_fn = factories.load_factory(config.factory_fn_ref)
    else:
        factory_fn = create_default_queries_factory(config, settings)
    async with factories.run_factory(factory_fn()) as q:
        yield q


async def display_stats(log_stats: list[models.LogStatistics]) -> None:
    print(
        tabulate(
            [
                (
                    stat.created.astimezone(),
                    stat.count,
                    stat.entrypoint,
                    stat.status,
                    stat.priority,
                )
                for stat in log_stats
            ],
            headers=["Created", "Count", "Entrypoint", "Status", "Priority"],
            tablefmt=os.environ.get(qb.add_prefix("TABLEFMT"), "pretty"),
        )
    )


async def display_pg_channel(
    connection: db.Driver,
    channel: models.Channel,
) -> None:
    queue = asyncio.Queue[models.AnyEvent]()
    await listeners.initialize_notice_event_listener(
        connection,
        channel,
        queue.put_nowait,
    )
    while True:
        event = await queue.get()
        print(repr(event.root))


async def display_schedule(schedules: list[models.Schedule]) -> None:
    print(
        tabulate(
            [
                (
                    x.id,
                    x.expression,
                    x.heartbeat.astimezone() if x.heartbeat else "",
                    x.created.astimezone() if x.created else "",
                    x.updated.astimezone() if x.updated else "",
                    x.next_run.astimezone() if x.next_run else "",
                    x.last_run.astimezone() if x.last_run else "",
                    x.status,
                    x.entrypoint,
                )
                for x in schedules
            ],
            headers=[
                "id",
                "expression",
                "heartbeat",
                "created",
                "updated",
                "next_run",
                "last_run",
                "status",
                "entrypoint",
            ],
            tablefmt=os.environ.get(qb.add_prefix("TABLEFMT"), "pretty"),
        )
    )


async def fetch_and_display(
    q: queries.Queries,
    interval: timedelta | None,
    tail: int,
) -> None:
    clear_and_home = "\033[2J\033[H"
    while True:
        print(clear_and_home, end="")
        await display_stats(await q.log_statistics(tail))
        if interval is None:
            return
        await asyncio.sleep(interval.total_seconds())


@app.command(help="Install the necessary database schema for PGQueuer.")
def install(
    ctx: Context,
    dry_run: bool = typer.Option(
        False,
        help="Print SQL only.",
    ),
    durability: qb.Durability = typer.Option(
        qb.Durability.durable.value,
        "--durability",
        "-d",
        help="Durability level for tables.",
    ),
) -> None:
    print(qb.QueryBuilderEnvironment(qb.DBSettings(durability=durability)).build_install_query())

    async def run() -> None:
        async with yield_queries(ctx, qb.DBSettings(durability=durability)) as q:
            await q.install()

    if not dry_run:
        asyncio_run(run())


@app.command(help="Remove the PGQueuer schema from the database.")
def uninstall(
    ctx: Context,
    dry_run: bool = typer.Option(
        False,
        help="Print SQL only.",
    ),
) -> None:
    print(qb.QueryBuilderEnvironment().build_uninstall_query())

    async def run() -> None:
        async with yield_queries(ctx, qb.DBSettings()) as q:
            await q.uninstall()

    if not dry_run:
        asyncio_run(run())


@app.command(help="Apply upgrades to the existing PGQueuer database schema.")
def upgrade(
    ctx: Context,
    dry_run: bool = typer.Option(
        False,
        help="Print SQL only.",
    ),
    durability: qb.Durability = typer.Option(
        qb.Durability.durable.value,
        "--durability",
        "-d",
        help="Durability level for tables.",
    ),
) -> None:
    print(f"\n{'-' * 50}\n".join(qb.QueryBuilderEnvironment().build_upgrade_queries()))

    async def run() -> None:
        async with yield_queries(ctx, qb.DBSettings(durability=durability)) as q:
            await q.upgrade()

    if not dry_run:
        asyncio_run(run())


@app.command(help="Display a live dashboard showing job statistics.")
def dashboard(
    ctx: Context,
    interval: float | None = typer.Option(
        None,
        "-i",
        "--interval",
    ),
    tail: int = typer.Option(
        25,
        "-n",
        "--tail",
    ),
) -> None:
    interval_td = timedelta(seconds=interval) if interval is not None else None

    async def run() -> None:
        async with yield_queries(ctx, qb.DBSettings()) as q:
            await fetch_and_display(q, interval_td, tail)

    asyncio_run(run())


@app.command(help="Listen to a PostgreSQL NOTIFY channel for debug purposes.")
def listen(
    ctx: Context,
    channel: str = typer.Option(
        qb.DBSettings().channel,
        "--channel",
    ),
) -> None:
    async def run() -> None:
        async with yield_queries(ctx, qb.DBSettings()) as q:
            await display_pg_channel(q.driver, models.Channel(channel))

    asyncio_run(run())


@app.command(help="Start a PGQueuer.")
def run(
    factory_fn: str = typer.Argument(
        ...,
        help="Path to a function returning a Queries instance.",
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
    """
    Run the job manager, pulling tasks from the queue and handling them with workers.
    """
    logconfig.setup_fancy_logger(log_level)

    asyncio_run(
        supervisor.runit(
            factories.load_factory(factory_fn),
            dequeue_timeout=timedelta(seconds=dequeue_timeout),
            batch_size=batch_size,
            restart_delay=timedelta(seconds=restart_delay if restart_on_failure else 0),
            restart_on_failure=restart_on_failure,
            shutdown=asyncio.Event(),
            mode=mode,
            max_concurrent_tasks=max_concurrent_tasks,
            shutdown_on_listener_failure=shutdown_on_listener_failure,
        )
    )


@app.command(help="Manage schedules in the PGQueuer system.")
def schedules(
    ctx: Context,
    remove: list[str] = typer.Option(
        [],
        "-r",
        "--remove",
        help="Remove schedules by ID or name.",
    ),
) -> None:
    async def run_async() -> None:
        async with yield_queries(ctx, qb.DBSettings()) as q:
            if remove:
                schedule_ids = {models.ScheduleId(int(x)) for x in remove if x.isdigit()}
                schedule_names = {types.CronEntrypoint(x) for x in remove if not x.isdigit()}
                await q.delete_schedule(schedule_ids, schedule_names)
            await display_schedule(await q.peak_schedule())

    asyncio_run(run_async())


@app.command(help="Manually enqueue a job into the PGQueuer system.")
def queue(
    ctx: Context,
    entrypoint: str = typer.Argument(
        ...,
        help="The entry point of the job to be executed.",
    ),
    payload: str | None = typer.Argument(
        None,
        help="Optional payload for the job, can be any serialized data.",
    ),
) -> None:
    async def run_async() -> None:
        async with yield_queries(ctx, qb.DBSettings()) as q:
            await q.enqueue(
                entrypoint,
                None if payload is None else payload.encode(),
                priority=0,
                execute_after=timedelta(seconds=0),
            )

    asyncio_run(run_async())


@app.command(help="Alter the logging durability for PGQueuer tables.")
def durability(
    ctx: Context,
    durability: qb.Durability = typer.Argument(
        ...,
        help=(
            "The durability mode to set: 'volatile' (all unlogged), "
            "'balanced' (main table logged, others unlogged), 'durable' (all logged)."
        ),
    ),
    dry_run: bool = typer.Option(
        False,
        help="Print SQL commands without executing them.",
    ),
) -> None:
    """
    Command to alter the durability level of the tables in PGQueuer without data loss.

    Args:
        ctx: Context object with configuration information.
        durability: The desired durability level ('volatile', 'balanced', or 'durable').
        dry_run: Whether to print SQL commands without executing them.
    """
    print(
        "\n".join(
            qb.QueryBuilderEnvironment(
                qb.DBSettings(durability=durability)
            ).build_alter_durability_query()
        )
    )

    async def run() -> None:
        async with yield_queries(ctx, qb.DBSettings(durability=durability)) as q:
            await q.alter_durability()

    if not dry_run:
        asyncio_run(run())


if __name__ == "__main__":
    app(prog_name="pgqueuer")
