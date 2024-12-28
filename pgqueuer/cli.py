from __future__ import annotations

import asyncio
import contextlib
import os
from dataclasses import dataclass
from datetime import timedelta

import typer
from tabulate import tabulate
from typer import Context

from . import db, helpers, listeners, models, qb, queries, supervisor

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
    prefix: str = typer.Option("", help="Prefix for pgqueuer objects.", envvar="PGQUEUER_PREFIX"),
    pg_dsn: str = typer.Option(None, help="Database DSN.", envvar="PGDSN"),
    pg_host: str = typer.Option(None, help="Database host.", envvar="PGHOST"),
    pg_port: str = typer.Option("5432", help="Database port.", envvar="PGPORT"),
    pg_user: str = typer.Option(None, help="Database user.", envvar="PGUSER"),
    pg_database: str = typer.Option(None, help="Database name.", envvar="PGDATABASE"),
    pg_password: str = typer.Option(None, help="Database password.", envvar="PGPASSWORD"),
    pg_schema: str = typer.Option(None, help="Database schema.", envvar="PGSCHEMA"),
) -> None:
    config = AppConfig(
        prefix=prefix,
        pg_dsn=pg_dsn,
        pg_host=pg_host,
        pg_port=pg_port,
        pg_user=pg_user,
        pg_database=pg_database,
        pg_password=pg_password,
        pg_schema=pg_schema,
    )
    config.setup_env()
    ctx.obj = config


async def display_stats(log_stats: list[models.LogStatistics]) -> None:
    print(
        tabulate(
            [
                (
                    stat.created.astimezone(),
                    stat.count,
                    stat.entrypoint,
                    stat.time_in_queue,
                    stat.status,
                    stat.priority,
                )
                for stat in log_stats
            ],
            headers=[
                "Created",
                "Count",
                "Entrypoint",
                "Time in Queue (HH:MM:SS)",
                "Status",
                "Priority",
            ],
            tablefmt=os.environ.get(qb.add_prefix("TABLEFMT"), "pretty"),
        )
    )


async def display_pg_channel(
    connection: db.Driver,
    channel: models.PGChannel,
) -> None:
    queue = asyncio.Queue[models.AnyEvent]()
    await listeners.initialize_notice_event_listener(
        connection,
        channel,
        queue.put_nowait,
    )
    while True:
        print(repr((await queue.get()).root))


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
    queries_obj: queries.Queries,
    interval: timedelta | None,
    tail: int,
) -> None:
    clear_and_home = "\033[2J\033[H"
    while True:
        print(clear_and_home, end="")
        await display_stats(await queries_obj.log_statistics(tail))
        if interval is None:
            return
        await asyncio.sleep(interval.total_seconds())


async def query_adapter(conninfo: str) -> queries.Queries:
    with contextlib.suppress(ImportError):
        import asyncpg

        return queries.Queries(db.AsyncpgDriver(await asyncpg.connect(dsn=conninfo)))
    with contextlib.suppress(ImportError):
        import psycopg

        return queries.Queries(
            db.PsycopgDriver(
                await psycopg.AsyncConnection.connect(conninfo=conninfo, autocommit=True)
            )
        )
    raise RuntimeError("Neither asyncpg nor psycopg could be imported.")


@app.command(help="Install the necessary database schema for PGQueuer.")
def install(
    ctx: Context,
    dry_run: bool = typer.Option(False, help="Print SQL only."),
) -> None:
    config: AppConfig = ctx.obj
    print(queries.qb.QueryBuilderEnvironment().create_install_query())

    async def run() -> None:
        await (await query_adapter(config.dsn)).install()

    if not dry_run:
        asyncio.run(run())


@app.command(help="Remove the PGQueuer schema from the database.")
def uninstall(
    ctx: Context,
    dry_run: bool = typer.Option(False, help="Print SQL only."),
) -> None:
    config: AppConfig = ctx.obj
    print(queries.qb.QueryBuilderEnvironment().create_uninstall_query())

    async def run() -> None:
        if not dry_run:
            await (await query_adapter(config.dsn)).uninstall()

    asyncio.run(run())


@app.command(help="Apply upgrades to the existing PGQueuer database schema.")
def upgrade(
    ctx: Context,
    dry_run: bool = typer.Option(False, help="Print SQL only."),
) -> None:
    config: AppConfig = ctx.obj
    print(f"\n{'-'*50}\n".join(queries.qb.QueryBuilderEnvironment().create_upgrade_queries()))

    async def run() -> None:
        if not dry_run:
            await (await query_adapter(config.dsn)).upgrade()

    asyncio.run(run())


@app.command(help="Display a live dashboard showing job statistics.")
def dashboard(
    ctx: Context,
    interval: float | None = typer.Option(None, "-i", "--interval"),
    tail: int = typer.Option(25, "-n", "--tail"),
) -> None:
    config: AppConfig = ctx.obj
    interval_td = timedelta(seconds=interval) if interval is not None else None

    async def run() -> None:
        await fetch_and_display(await query_adapter(config.dsn), interval_td, tail)

    asyncio.run(run())


@app.command(help="Listen to a PostgreSQL NOTIFY channel for debug purposes.")
def listen(
    ctx: Context,
    channel: str = typer.Option(qb.DBSettings().channel, "--channel"),
) -> None:
    config: AppConfig = ctx.obj

    async def run() -> None:
        await display_pg_channel(
            (await query_adapter(config.dsn)).driver, models.PGChannel(channel)
        )

    asyncio.run(run())


@app.command(help="Start a QueueManager to manage and process jobs.")
def run(
    factory_fn: str = typer.Argument(...),
    dequeue_timeout: float = typer.Option(30.0, "--dequeue-timeout"),
    batch_size: int = typer.Option(10, "--batch-size"),
    restart_delay: float = typer.Option(5.0, "--restart-delay"),
    restart_on_failure: bool = typer.Option(False, "--restart-on-failure"),
) -> None:
    asyncio.run(
        supervisor.runit(
            factory_fn,
            dequeue_timeout=timedelta(seconds=dequeue_timeout),
            batch_size=batch_size,
            restart_delay=timedelta(seconds=restart_delay if restart_on_failure else 0),
            restart_on_failure=restart_on_failure,
            shutdown=asyncio.Event(),
        )
    )


@app.command(help="Manage schedules in the PGQueuer system.")
def schedules(
    ctx: Context,
    remove: list[str] = typer.Option([], "-r", "--remove"),
) -> None:
    config: AppConfig = ctx.obj

    async def run_async() -> None:
        q = await query_adapter(config.dsn)
        if remove:
            schedule_ids = {models.ScheduleId(int(x)) for x in remove if x.isdigit()}
            schedule_names = {x for x in remove if not x.isdigit()}
            await q.delete_schedule(schedule_ids, schedule_names)
        await display_schedule(await q.peak_schedule())

    asyncio.run(run_async())


if __name__ == "__main__":
    app(prog_name="pgqueuer")
