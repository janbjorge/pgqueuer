from __future__ import annotations

import asyncio
import contextlib
import functools
import os
import sys
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from typing import TYPE_CHECKING, Callable, Coroutine

import typer
from tabulate import tabulate
from typer import Context
from typing_extensions import AsyncGenerator, assert_never

from pgqueuer.adapters.cli import factories, sql_cmd, supervisor
from pgqueuer.adapters.persistence import qb, queries
from pgqueuer.core import listeners, logconfig
from pgqueuer.domain import models, types
from pgqueuer.ports.driver import Driver

if TYPE_CHECKING:
    import uvloop

try:
    import uvloop  # noqa: F811

    HAS_UVLOOP = True
except ImportError:
    HAS_UVLOOP = False


def asyncio_run(coro: Coroutine[object, object, object]) -> None:
    """Run *coro* on the best event loop for this platform."""
    if sys.platform != "win32":
        if HAS_UVLOOP:
            uvloop.run(coro)
        else:
            asyncio.run(coro)
        return

    # psycopg async rejects ProactorEventLoop (Windows default); force the
    # selector loop on every supported Windows + Python combination.
    if sys.version_info >= (3, 12):
        asyncio.run(coro, loop_factory=asyncio.SelectorEventLoop)
        return
    if sys.version_info >= (3, 11):
        with asyncio.Runner(loop_factory=asyncio.SelectorEventLoop) as runner:
            runner.run(coro)
        return
    # Python 3.10: no Runner, no loop_factory; mutate policy.
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(coro)


app = typer.Typer(
    help=(
        "PgQueuer CLI: Manage and monitor recurring schedules with PostgreSQL, "
        "featuring dashboards, real-time tracking, and schema tools."
    ),
    epilog="Explore documentation and examples: https://github.com/janbjorge/pgqueuer",
    no_args_is_help=True,
    pretty_exceptions_show_locals=False,
    add_completion=False,
)
app.add_typer(sql_cmd.sql_app, name="sql")


def emit_deprecated_dry_run(ctx: Context, sql: str) -> None:
    typer.secho(
        "Warning: --dry-run is deprecated and will be removed in v2.0; "
        f"use 'pgq sql {ctx.command.name}'.",
        err=True,
        fg=typer.colors.YELLOW,
    )
    typer.echo(sql)


class VerifyMode(Enum):
    PRESENT = "present"
    ABSENT = "absent"


class OnConflictChoice(Enum):
    RAISE = "raise"
    SKIP = "skip"


@dataclass
class AppConfig:
    prefix: str = ""
    schema: str = ""
    pg_dsn: str = ""
    factory_fn_ref: str | None = None

    def setup_env(self) -> None:
        if self.prefix:
            os.environ["PGQUEUER_PREFIX"] = self.prefix
        if self.schema:
            os.environ["PGQUEUER_SCHEMA"] = self.schema


@app.callback()
def main(
    ctx: Context,
    prefix: str = typer.Option(
        "",
        help="Prefix for pgqueuer objects.",
        envvar="PGQUEUER_PREFIX",
    ),
    schema: str = typer.Option(
        "",
        help="Postgres schema holding all pgqueuer objects.",
        envvar="PGQUEUER_SCHEMA",
    ),
    pg_dsn: str = typer.Option(
        "",
        help=(
            "PostgreSQL connection string (DSN). "
            "When omitted, PGQUEUER_DSN and standard libpq env vars "
            "(PGHOST, PGUSER, PGPASSWORD, PGDATABASE, PGPORT) are used. "
            "Use --schema/PGQUEUER_SCHEMA to target a Postgres schema."
        ),
        envvar="PGDSN",
    ),
    factory_fn_ref: str | None = typer.Option(
        None,
        "--factory",
        help="A reference to a function that returns an instance of Queries",
    ),
) -> None:
    config = AppConfig(
        prefix=prefix,
        schema=schema,
        pg_dsn=pg_dsn,
        factory_fn_ref=factory_fn_ref,
    )
    config.setup_env()
    ctx.obj = config


def create_default_queries_factory(
    config: AppConfig,
    settings: qb.DBSettings,
) -> Callable[..., contextlib.AbstractAsyncContextManager[queries.Queries]]:
    """Default Queries factory: try asyncpg, fall back to psycopg."""

    @contextlib.asynccontextmanager
    async def factory() -> AsyncGenerator[queries.Queries, None]:
        with contextlib.suppress(ImportError):
            from pgqueuer.adapters.connections import connect_asyncpg
            from pgqueuer.adapters.drivers.asyncpg import AsyncpgDriver

            async with connect_asyncpg(dsn=config.pg_dsn or None) as connection:
                yield queries.Queries(
                    AsyncpgDriver(connection),
                    qbe=qb.QueryBuilderEnvironment(settings),
                    qbq=qb.QueryQueueBuilder(settings),
                    qbs=qb.QuerySchedulerBuilder(settings),
                )
            return
        with contextlib.suppress(ImportError):
            from pgqueuer.adapters.connections import connect_psycopg
            from pgqueuer.adapters.drivers.psycopg import PsycopgDriver

            async with connect_psycopg(dsn=config.pg_dsn or None) as connection:
                yield queries.Queries(
                    PsycopgDriver(connection),
                    qbe=qb.QueryBuilderEnvironment(settings),
                    qbq=qb.QueryQueueBuilder(settings),
                    qbs=qb.QuerySchedulerBuilder(settings),
                )
            return
        raise RuntimeError("Neither asyncpg nor psycopg could be imported.")

    return factory


@contextlib.asynccontextmanager
async def yield_queries(
    ctx: Context,
    settings: qb.DBSettings,
) -> AsyncGenerator[queries.Queries, None]:
    """Yield Queries from the user-supplied factory or the built-in default."""
    config: AppConfig = ctx.obj
    if config.factory_fn_ref:
        factory_fn = factories.load_factory(config.factory_fn_ref)
    else:
        factory_fn = create_default_queries_factory(config, settings)
    async with factories.validate_factory_result(factory_fn()) as q:
        yield q


def tablefmt() -> str:
    """Tabulate table format; PGQUEUER_TABLEFMT preferred, legacy TABLEFMT honored."""
    return os.environ.get("PGQUEUER_TABLEFMT", os.environ.get("TABLEFMT", "pretty"))


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
            tablefmt=tablefmt(),
        )
    )


async def display_pg_channel(
    connection: Driver,
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
            tablefmt=tablefmt(),
        )
    )


async def fetch_and_display(
    q: queries.Queries,
    interval: timedelta | None,
    limit: int,
) -> None:
    clear_and_home = "\033[2J\033[H"
    while True:
        print(clear_and_home, end="")
        await display_stats(await q.log_statistics(limit))
        if interval is None:
            return
        await asyncio.sleep(interval.total_seconds())


@app.command(help="Install the necessary database schema for PgQueuer.")
def install(
    ctx: Context,
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        hidden=True,
        help="Deprecated: use 'pgq sql install'.",
    ),
    durability: sql_cmd.DurabilityOption = qb.Durability.durable,
    create_schema: sql_cmd.CreateSchemaOption = True,
) -> None:
    settings = qb.DBSettings(durability=durability)
    if dry_run:
        emit_deprecated_dry_run(ctx, sql_cmd.render_install(settings, create_schema))
        return

    async def run() -> None:
        async with yield_queries(ctx, settings) as q:
            await q.install(create_schema=create_schema)

    asyncio_run(run())
    typer.secho(f"Installed PgQueuer schema (durability={durability.value}).", err=True)


@app.command(help="Verify PgQueuer database objects.")
def verify(
    ctx: Context,
    expect: VerifyMode = typer.Option(
        ...,
        help="Expected object state: 'present' or 'absent'.",
    ),
) -> None:
    async def run() -> None:
        async with yield_queries(ctx, qb.DBSettings()) as q:
            expect_present = expect == VerifyMode.PRESENT
            divergence = list[str]()

            required_tables = [
                q.qbe.settings.queue_table,
                q.qbe.settings.statistics_table,
                q.qbe.settings.schedules_table,
                q.qbe.settings.queue_table_log,
            ]

            for table in required_tables:
                exists = await q.has_table(table)
                if expect_present != exists:
                    state = "missing" if expect_present else "unexpected"
                    divergence.append(f"{state} table '{table}'")

            func_exists = await q.has_function(q.qbe.settings.function)
            if expect_present != func_exists:
                state = "missing" if expect_present else "unexpected"
                divergence.append(f"{state} function '{q.qbe.settings.function}'")

            trig_exists = await q.has_trigger(q.qbe.settings.trigger)
            if expect_present != trig_exists:
                state = "missing" if expect_present else "unexpected"
                divergence.append(f"{state} trigger '{q.qbe.settings.trigger}'")

            if divergence:
                print("\n".join(divergence))
            else:
                if expect == VerifyMode.PRESENT:
                    print("All required PgQueuer database objects are present.")
                else:
                    print("No PgQueuer database objects found")

            raise typer.Exit(code=1 if divergence else 0)

    asyncio_run(run())


@app.command(help="Remove the PgQueuer schema from the database.")
def uninstall(
    ctx: Context,
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        hidden=True,
        help="Deprecated: use 'pgq sql uninstall'.",
    ),
) -> None:
    if dry_run:
        emit_deprecated_dry_run(ctx, sql_cmd.render_uninstall())
        return

    async def run() -> None:
        async with yield_queries(ctx, qb.DBSettings()) as q:
            await q.uninstall()

    asyncio_run(run())
    typer.secho("Uninstalled PgQueuer schema.", err=True)


@app.command(help="Apply upgrades to the existing PgQueuer database schema.")
def upgrade(
    ctx: Context,
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        hidden=True,
        help="Deprecated: use 'pgq sql upgrade'.",
    ),
    durability: sql_cmd.DurabilityOption = qb.Durability.durable,
    widen_id: sql_cmd.WidenIdOption = True,
) -> None:
    settings = qb.DBSettings(durability=durability, widen_id=widen_id)
    if dry_run:
        emit_deprecated_dry_run(ctx, sql_cmd.render_upgrade(settings))
        return

    async def run() -> None:
        async with yield_queries(ctx, settings) as q:
            await q.upgrade()

    asyncio_run(run())
    typer.secho("Upgraded PgQueuer schema.", err=True)


@app.command(help="Display a live dashboard showing job statistics.")
def dashboard(
    ctx: Context,
    interval: float | None = typer.Option(
        None,
        "-i",
        "--interval",
    ),
    limit: int = typer.Option(
        25,
        "-n",
        "--limit",
    ),
) -> None:
    interval_td = timedelta(seconds=interval) if interval is not None else None

    async def run() -> None:
        async with yield_queries(ctx, qb.DBSettings()) as q:
            await fetch_and_display(q, interval_td, limit)

    asyncio_run(run())


@app.command(help="Serve the web dashboard (requires the 'web' extra).")
def web(
    ctx: Context,
    host: str = typer.Option(
        "127.0.0.1",
        "--host",
        envvar="PGQUEUER_WEB_HOST",
        help="Bind address. Use 0.0.0.0 to expose beyond localhost.",
    ),
    port: int = typer.Option(
        8080,
        "--port",
        envvar="PGQUEUER_WEB_PORT",
    ),
) -> None:
    try:
        import uvicorn

        from pgqueuer.adapters.web.app import create_web_app
    except ImportError as exc:
        raise typer.BadParameter(
            "The web dashboard requires extra dependencies. "
            "Install with: pip install pgqueuer[web,asyncpg]"
        ) from exc

    logconfig.setup_fancy_logger(logconfig.LogLevel.INFO)
    config: AppConfig = ctx.obj
    uvicorn.run(create_web_app(config.pg_dsn or None), host=host, port=port)


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


@app.command(help="Start a PgQueuer.")
def run(
    factory_fn: str = typer.Argument(
        ...,
        help="Path to a factory function (module:function).",
    ),
    factory_args: list[str] = typer.Argument(
        None,
        help="Extra arguments forwarded to the factory (pass after --).",
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
    heartbeat_timeout: float = typer.Option(
        30.0,
        "--heartbeat-timeout",
        help="Seconds without a heartbeat before a job is re-picked by another worker.",
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

    Extra arguments after -- are forwarded to the factory function.
    """
    logconfig.setup_fancy_logger(log_level)

    factory = factories.load_factory(factory_fn)
    if factory_args:
        factory = functools.partial(factory, factory_args)

    asyncio_run(
        supervisor.runit(
            factory,
            dequeue_timeout=timedelta(seconds=dequeue_timeout),
            batch_size=batch_size,
            restart_delay=timedelta(seconds=restart_delay if restart_on_failure else 0),
            restart_on_failure=restart_on_failure,
            shutdown=asyncio.Event(),
            mode=mode,
            max_concurrent_tasks=max_concurrent_tasks,
            shutdown_on_listener_failure=shutdown_on_listener_failure,
            heartbeat_timeout=timedelta(seconds=heartbeat_timeout),
        )
    )


@app.command(help="Manage schedules in the PgQueuer system.")
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
            await display_schedule(await q.peek_schedule())

    asyncio_run(run_async())


@app.command(help="Manually enqueue a job into the PgQueuer system.")
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
    dedupe_key: str | None = typer.Option(
        None,
        "--dedupe-key",
        help="Deduplication key; an active ('queued'/'picked') job with the same key blocks it.",
    ),
    on_conflict: OnConflictChoice = typer.Option(
        OnConflictChoice.RAISE,
        "--on-conflict",
        help="On dedupe-key conflict: 'raise' exits with an error, 'skip' exits 0 silently.",
    ),
) -> None:
    async def run_async() -> None:
        # For a single job, skip mode subsumes raise mode: a None result is
        # exactly the duplicate case, so on_conflict only decides the exit.
        async with yield_queries(ctx, qb.DBSettings()) as q:
            (job_id,) = await q.enqueue(
                entrypoint,
                None if payload is None else payload.encode(),
                priority=0,
                execute_after=timedelta(seconds=0),
                dedupe_key=dedupe_key,
                on_conflict="skip",
            )

        if job_id is not None:
            print(f"Enqueued job {job_id}.")
        elif on_conflict is OnConflictChoice.SKIP:
            print(f"Skipped: duplicate dedupe_key {dedupe_key!r}.")
        elif on_conflict is OnConflictChoice.RAISE:
            print(f"Error: duplicate dedupe_key {dedupe_key!r}.")
            raise typer.Exit(code=1)
        else:
            assert_never(on_conflict)

    asyncio_run(run_async())


@app.command(help="List jobs held with status 'failed' for manual intervention.")
def failed(
    ctx: Context,
    limit: int = typer.Option(25, "-n", "--limit", help="Maximum number of jobs to display."),
) -> None:
    async def run() -> None:
        async with yield_queries(ctx, qb.DBSettings()) as q:
            jobs = await q.list_failed_jobs(limit=limit)
            if not jobs:
                print("No failed jobs.")
                return
            rows = [
                [
                    j.id,
                    j.entrypoint,
                    j.attempts,
                    j.created.strftime("%Y-%m-%d %H:%M:%S"),
                    len(j.payload) if j.payload else 0,
                ]
                for j in jobs
            ]
            print(
                tabulate(
                    rows,
                    headers=["ID", "Entrypoint", "Attempts", "Created", "Payload bytes"],
                    tablefmt=tablefmt(),
                )
            )

    asyncio_run(run())


@app.command(help="Re-queue failed jobs by ID so they can be processed again.")
def requeue(
    ctx: Context,
    ids: list[int] = typer.Argument(..., help="Job IDs to re-queue."),
) -> None:
    async def run() -> None:
        async with yield_queries(ctx, qb.DBSettings()) as q:
            typed_ids = [types.JobId(i) for i in ids]
            await q.requeue_jobs(typed_ids)
            print(f"Re-queued {len(typed_ids)} job(s).")

    asyncio_run(run())


@app.command(help="Alter the logging durability for PgQueuer tables.")
def durability(
    ctx: Context,
    durability: sql_cmd.DurabilityArgument,
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        hidden=True,
        help="Deprecated: use 'pgq sql durability'.",
    ),
) -> None:
    """Switch durability level of PgQueuer tables without data loss."""
    settings = qb.DBSettings(durability=durability)
    if dry_run:
        emit_deprecated_dry_run(ctx, sql_cmd.render_durability(settings))
        return

    async def run() -> None:
        async with yield_queries(ctx, settings) as q:
            await q.alter_durability()

    asyncio_run(run())
    typer.secho(f"Set PgQueuer durability to {durability.value}.", err=True)


@app.command(name="autovac", help="Optimize autovacuum settings for PgQueuer tables.")
def optimize_autovacuum(
    ctx: Context,
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        hidden=True,
        help="Deprecated: use 'pgq sql autovac'.",
    ),
    rollback: bool = typer.Option(False, help="Reset to defaults instead."),
) -> None:
    """Apply or revert recommended autovacuum settings."""
    if dry_run:
        emit_deprecated_dry_run(ctx, sql_cmd.render_autovac(rollback))
        return

    async def run() -> None:
        async with yield_queries(ctx, qb.DBSettings()) as q:
            await (q.optimize_autovacuum_rollback() if rollback else q.optimize_autovacuum())

    asyncio_run(run())
    typer.secho(
        "Reset autovacuum settings to defaults."
        if rollback
        else "Applied recommended autovacuum settings.",
        err=True,
    )


if __name__ == "__main__":
    app(prog_name="pgqueuer")
