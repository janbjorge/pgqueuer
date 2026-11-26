from __future__ import annotations

import asyncio
import contextlib
import functools
import json
import os
import sys
from collections.abc import AsyncGenerator, Callable, Coroutine, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import TYPE_CHECKING, Annotated, TypeVar, assert_never

import typer
from pydantic_core import to_json
from tabulate import tabulate
from typer import Context

from pgqueuer.adapters.cli import factories, sql_cmd, supervisor
from pgqueuer.adapters.connections import connect_asyncpg, connect_psycopg
from pgqueuer.adapters.drivers.asyncpg import AsyncpgDriver
from pgqueuer.adapters.drivers.psycopg import PsycopgDriver
from pgqueuer.adapters.persistence import qb, queries
from pgqueuer.core import listeners, logconfig
from pgqueuer.core.insights import InsightsService
from pgqueuer.domain import errors, models, types
from pgqueuer.domain.schema import model as schema_model
from pgqueuer.domain.settings import DBSettings
from pgqueuer.ports.driver import Driver

if TYPE_CHECKING:
    import uvloop

try:
    import uvloop

    HAS_UVLOOP = True
except ImportError:
    HAS_UVLOOP = False


T = TypeVar("T")


def asyncio_run(coro: Coroutine[object, object, T]) -> T:
    """Run *coro* on the best event loop for this platform and return its result."""
    if sys.platform == "win32":
        # psycopg async rejects ProactorEventLoop (Windows default); force the
        # selector loop on every supported Windows + Python combination.
        with asyncio.Runner(loop_factory=asyncio.SelectorEventLoop) as runner:
            return runner.run(coro)
    if HAS_UVLOOP:
        return uvloop.run(coro)
    return asyncio.run(coro)


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


Limit = Annotated[int, typer.Option("-n", "--limit", help="Maximum number of rows to display.")]
JsonFlag = Annotated[
    bool, typer.Option("--json", help="Print the result as a JSON array on stdout.")
]


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
    # Names for --prefix/--schema; the remaining names come from PGQUEUER_* env
    # vars. Every command reads this one object.
    settings: DBSettings = field(init=False)

    def __post_init__(self) -> None:
        self.settings = DBSettings(prefix=self.prefix, db_schema=self.schema or None)

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
            "If you do not give a DSN, PgQueuer uses PGQUEUER_DSN, then PGDSN. "
            "Then it uses the standard libpq env vars "
            "(PGHOST, PGUSER, PGPASSWORD, PGDATABASE, PGPORT). "
            "Use --schema/PGQUEUER_SCHEMA to target a Postgres schema."
        ),
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
) -> Callable[[], contextlib.AbstractAsyncContextManager[queries.Queries]]:
    """Default Queries factory: try asyncpg, fall back to psycopg."""

    @contextlib.asynccontextmanager
    async def factory() -> AsyncGenerator[queries.Queries, None]:
        with contextlib.suppress(ImportError):
            async with connect_asyncpg(dsn=config.pg_dsn or None) as connection:
                yield queries.Queries(AsyncpgDriver(connection), config.settings)
            return
        with contextlib.suppress(ImportError):
            async with connect_psycopg(dsn=config.pg_dsn or None) as connection:
                yield queries.Queries(PsycopgDriver(connection), config.settings)
            return
        raise RuntimeError("Neither asyncpg nor psycopg could be imported.")

    return factory


@contextlib.asynccontextmanager
async def yield_queries(ctx: Context) -> AsyncGenerator[queries.Queries, None]:
    """Yield Queries from the user-supplied factory or the built-in default."""
    config: AppConfig = ctx.obj
    factory_fn: Callable[[], object]
    if config.factory_fn_ref:
        factory_fn = factories.load_factory(config.factory_fn_ref)
    else:
        factory_fn = create_default_queries_factory(config)
    async with factories.validate_factory_result(factory_fn()) as entered:
        if not isinstance(entered, queries.Queries):
            raise TypeError(f"Factory must yield Queries, got {type(entered).__name__}")
        yield entered


def tablefmt() -> str:
    """Tabulate table format; PGQUEUER_TABLEFMT preferred, legacy TABLEFMT honored."""
    return os.environ.get("PGQUEUER_TABLEFMT", os.environ.get("TABLEFMT", "pretty"))


def local_time(value: datetime) -> str:
    """Terminal-local time with its offset, whatever zone the driver returned."""
    return value.astimezone().isoformat(sep=" ", timespec="seconds")


def job_json(job: models.Job) -> dict[str, object]:
    """A job for --json output; payloads are arbitrary bytes, so only their size is kept."""
    return job.model_dump(mode="json", exclude={"payload"}) | {
        "payload_bytes": len(job.payload) if job.payload else 0
    }


def print_report(
    items: Sequence[T],
    as_json: bool,
    empty: str,
    headers: list[str],
    row: Callable[[T], list[object]],
    json_item: Callable[[T], object] | None = None,
) -> None:
    """Print *items* as a JSON array, or as a table with *empty* in place of no rows."""
    if as_json:
        print(to_json([json_item(i) for i in items] if json_item else items).decode())
    elif not items:
        print(empty)
    else:
        print(tabulate([row(i) for i in items], headers=headers, tablefmt=tablefmt()))


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
    channel: types.Channel,
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
                    x.heartbeat.astimezone(),
                    x.created.astimezone(),
                    x.updated.astimezone(),
                    x.next_run.astimezone(),
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


def located(settings: qb.DBSettings) -> str:
    """Which installation a message is about, when several share a database."""
    where = settings.db_schema or "the connection's search_path"
    return f"prefix={settings.prefix!r}, schema={where}"


@app.command(help="Install the necessary database schema for PgQueuer.")
def install(
    ctx: Context,
    durability: sql_cmd.DurabilityOption = None,
    create_schema: sql_cmd.CreateSchemaOption = True,
) -> None:
    config: AppConfig = ctx.obj
    durability = config.settings.durability if durability is None else durability
    settings = config.settings.model_copy(update={"durability": durability})

    async def run() -> bool:
        async with yield_queries(ctx) as q:
            if await q.schema_is_installed():
                return False
            await q.install(create_schema=create_schema, durability=durability)
            return True

    if not asyncio_run(run()):
        typer.secho(
            f"PgQueuer is already installed ({located(settings)}). "
            "Run 'pgq upgrade' to bring it up to date.",
            err=True,
        )
        raise typer.Exit(1)
    typer.secho(f"Installed PgQueuer schema (durability={durability.value}).", err=True)


async def verify_present(q: queries.Queries) -> int:
    """Exit code 1 only for objects that stop a worker; a missing speed-only index warns."""
    found = await q.missing_schema()
    for entry in found.speed_only:
        print(f"warning: missing {entry.describe()}, queries run slower without it")
    for entry in found.blocking:
        print(f"missing {entry.describe()}")
    if not found.blocking:
        print("All required PgQueuer database objects are present.")
    return 1 if found.blocking else 0


async def verify_absent(q: queries.Queries) -> int:
    settings = q.qbe.settings
    divergence = [
        f"unexpected table '{table}'"
        for table in (
            settings.queue_table,
            settings.statistics_table,
            settings.schedules_table,
            settings.queue_table_log,
        )
        if await q.has_table(table)
    ]
    if await q.has_function(settings.function):
        divergence.append(f"unexpected function '{settings.function}'")
    if await q.has_trigger(settings.trigger):
        divergence.append(f"unexpected trigger '{settings.trigger}'")
    print("\n".join(divergence) if divergence else "No PgQueuer database objects found")
    return 1 if divergence else 0


@app.command(help="Verify PgQueuer database objects.")
def verify(
    ctx: Context,
    expect: VerifyMode = typer.Option(
        ...,
        help="Expected object state: 'present' or 'absent'.",
    ),
) -> None:
    async def run() -> None:
        async with yield_queries(ctx) as q:
            if expect is VerifyMode.PRESENT:
                code = await verify_present(q)
            elif expect is VerifyMode.ABSENT:
                code = await verify_absent(q)
            else:
                assert_never(expect)
            raise typer.Exit(code=code)

    asyncio_run(run())


@app.command(help="Remove the PgQueuer schema from the database.")
def uninstall(
    ctx: Context,
) -> None:
    async def run() -> None:
        async with yield_queries(ctx) as q:
            await q.uninstall()

    asyncio_run(run())
    typer.secho("Uninstalled PgQueuer schema.", err=True)


def report_upgrade(applied: schema_model.Plan, planned_only: bool) -> None:
    """Summary and notes on stderr, so ``--plan`` can be redirected as SQL."""
    for note in applied.notes:
        typer.secho(f"note: {note}", err=True)
    if not applied.statements:
        typer.secho("PgQueuer schema is already up to date.", err=True)
        return
    count = len(applied.statements)
    verb = "Would apply" if planned_only else "Applied"
    typer.secho(f"{verb} {count} statement{'' if count == 1 else 's'}.", err=True)


@app.command(
    help=(
        "Bring the schema up to what this release declares. "
        "Use --plan to see the delta this database needs without applying it."
    )
)
def upgrade(
    ctx: Context,
    plan: bool = typer.Option(
        False,
        "--plan",
        help="Print the exact delta this database needs and exit without applying it.",
    ),
    widen_id: sql_cmd.WidenIdOption = None,
) -> None:
    config: AppConfig = ctx.obj
    widen_id = config.settings.widen_id if widen_id is None else widen_id
    settings = config.settings.model_copy(update={"widen_id": widen_id})

    async def run() -> schema_model.Plan:
        async with yield_queries(ctx) as q:
            return await (q.plan_upgrade if plan else q.apply_upgrade)(widen_id)

    try:
        applied = asyncio_run(run())
    except errors.SchemaDriftError as drift:
        # The one failure here addressed to an operator rather than a developer.
        typer.secho(f"error: {drift}", err=True, fg=typer.colors.RED)
        raise typer.Exit(1) from None
    if plan and (rendered := sql_cmd.render_plan(applied.statements, settings)):
        typer.echo(rendered)
    report_upgrade(applied, planned_only=plan)


@app.command(help="Display a live dashboard showing job statistics.")
def dashboard(
    ctx: Context,
    interval: float | None = typer.Option(
        None,
        "-i",
        "--interval",
    ),
    limit: Limit = 25,
    as_json: JsonFlag = False,
) -> None:
    if as_json and interval is not None:
        raise typer.BadParameter("--json prints once; drop --interval.")
    interval_td = timedelta(seconds=interval) if interval is not None else None

    async def run() -> None:
        async with yield_queries(ctx) as q:
            if as_json:
                print(to_json(await q.log_statistics(limit)).decode())
            else:
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
        import uvicorn  # noqa: PLC0415 - optional `web` extra

        from pgqueuer.adapters.web.app import create_web_app  # noqa: PLC0415 - needs fastapi
    except ImportError as exc:
        raise typer.BadParameter(
            "The web dashboard requires extra dependencies. "
            "Install with: pip install pgqueuer[web,asyncpg]"
        ) from exc

    logconfig.setup_fancy_logger(logconfig.LogLevel.INFO)
    config: AppConfig = ctx.obj
    uvicorn.run(
        create_web_app(config.pg_dsn or None, settings=config.settings), host=host, port=port
    )


@app.command(help="Listen to a PostgreSQL NOTIFY channel for debug purposes.")
def listen(
    ctx: Context,
    channel: str | None = typer.Option(
        None,
        "--channel",
        help="Channel to listen on; defaults to the one PgQueuer notifies on.",
    ),
) -> None:
    async def run() -> None:
        async with yield_queries(ctx) as q:
            await display_pg_channel(q.driver, types.Channel(channel or q.qbe.settings.channel))

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
        case_sensitive=False,
    ),
    mode: types.QueueExecutionMode = typer.Option(
        types.QueueExecutionMode.continuous.name,
        "--mode",
        help="Queue execution mode.",
    ),
    max_concurrent_tasks: int | None = typer.Option(
        None,
        "--max-concurrent-tasks",
        help="Most jobs this runner holds at once. Default max(100, 2 x batch size); 0: unlimited.",
    ),
    shutdown_on_listener_failure: bool = typer.Option(
        False,
        "--shutdown-on-listener-failure",
        help="Shutdown the manager if the listener fails.",
    ),
) -> None:
    """Run the job manager, pulling tasks from the queue and handling them with workers.

    Extra arguments after -- are forwarded to the factory function.
    """
    logconfig.setup_fancy_logger(log_level)

    factory = factories.load_factory(factory_fn)
    if factory_args:
        factory = functools.partial(factory, factory_args)

    asyncio_run(
        supervisor.run(
            factory,
            dequeue_timeout=timedelta(seconds=dequeue_timeout),
            batch_size=batch_size,
            restart_delay=timedelta(seconds=restart_delay),
            restart_on_failure=restart_on_failure,
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
    as_json: JsonFlag = False,
) -> None:
    async def run_async() -> None:
        async with yield_queries(ctx) as q:
            if remove:
                schedule_ids = {types.ScheduleId(int(x)) for x in remove if x.isdigit()}
                schedule_names = {types.CronEntrypoint(x) for x in remove if not x.isdigit()}
                await q.delete_schedule(schedule_ids, schedule_names)
            current = await q.peek_schedule()
            if as_json:
                print(to_json(current).decode())
            else:
                await display_schedule(current)

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
        async with yield_queries(ctx) as q:
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
            typer.secho(f"Error: duplicate dedupe_key {dedupe_key!r}.", err=True)
            raise typer.Exit(code=1)
        else:
            assert_never(on_conflict)

    asyncio_run(run_async())


@app.command(
    help=(
        "List jobs held with status 'failed' for manual intervention. "
        "With --json, payloads are reported by size only."
    )
)
def failed(
    ctx: Context,
    limit: Limit = 25,
    as_json: JsonFlag = False,
) -> None:
    async def run() -> None:
        async with yield_queries(ctx) as q:
            jobs = await q.list_failed_jobs(limit=limit)
            if as_json:
                print(json.dumps([job_json(j) for j in jobs]))
                return
            if not jobs:
                print("No failed jobs.")
                return
            rows = [
                [
                    j.id,
                    j.entrypoint,
                    j.attempts,
                    local_time(j.created),
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
        async with yield_queries(ctx) as q:
            typed_ids = [types.JobId(i) for i in ids]
            await q.requeue_jobs(typed_ids)
            print(f"Re-queued {len(typed_ids)} job(s).")

    asyncio_run(run())


@app.command(help="List picked jobs whose heartbeat is older than the threshold.")
def stale(
    ctx: Context,
    threshold: float = typer.Option(
        300,
        "-t",
        "--threshold",
        help="Seconds since the last heartbeat before a picked job counts as stale.",
    ),
    limit: Limit = 25,
    as_json: JsonFlag = False,
) -> None:
    async def run() -> None:
        async with yield_queries(ctx) as q:
            jobs = await InsightsService(q).stale_jobs(timedelta(seconds=threshold), limit)
            if as_json:
                print(to_json(jobs).decode())
                return
            if not jobs:
                print("No stale jobs.")
                return
            rows = [
                [
                    j.id,
                    j.entrypoint,
                    j.queue_manager_id,
                    local_time(j.heartbeat),
                    round(j.seconds_since_heartbeat),
                ]
                for j in jobs
            ]
            print(
                tabulate(
                    rows,
                    headers=[
                        "ID",
                        "Entrypoint",
                        "Queue manager",
                        "Heartbeat",
                        "Seconds since heartbeat",
                    ],
                    tablefmt=tablefmt(),
                )
            )

    asyncio_run(run())


@app.command(help="List queue managers currently holding picked jobs.")
def workers(
    ctx: Context,
    as_json: JsonFlag = False,
) -> None:
    async def run() -> None:
        async with yield_queries(ctx) as q:
            active = await InsightsService(q).active_workers()
            if as_json:
                print(to_json(active).decode())
                return
            if not active:
                print("No workers are holding picked jobs.")
                return
            rows = [
                [
                    w.queue_manager_id,
                    w.active_jobs,
                    local_time(w.oldest_heartbeat),
                    local_time(w.newest_heartbeat),
                    ", ".join(w.entrypoints),
                ]
                for w in active
            ]
            print(
                tabulate(
                    rows,
                    headers=[
                        "Queue manager",
                        "Picked jobs",
                        "Oldest heartbeat",
                        "Newest heartbeat",
                        "Entrypoints",
                    ],
                    tablefmt=tablefmt(),
                )
            )

    asyncio_run(run())


@app.command(help="Show how many jobs wait in 'queued' per entrypoint, and how old they are.")
def backlog(
    ctx: Context,
    as_json: JsonFlag = False,
) -> None:
    async def run() -> None:
        async with yield_queries(ctx) as q:
            ages = await InsightsService(q).queue_age()
            if as_json:
                print(to_json(ages).decode())
                return
            if not ages:
                print("No queued jobs.")
                return
            rows = [
                [
                    a.entrypoint,
                    a.queued_count,
                    local_time(a.oldest_created),
                    round(a.oldest_age_seconds),
                    round(a.avg_age_seconds),
                ]
                for a in ages
            ]
            print(
                tabulate(
                    rows,
                    headers=[
                        "Entrypoint",
                        "Queued",
                        "Oldest created",
                        "Oldest age (s)",
                        "Average age (s)",
                    ],
                    tablefmt=tablefmt(),
                )
            )

    asyncio_run(run())


@app.command(help="Count jobs in the queue table per entrypoint, status and priority.")
def size(
    ctx: Context,
    as_json: JsonFlag = False,
) -> None:
    async def run() -> None:
        async with yield_queries(ctx) as q:
            sizes = await InsightsService(q).queue_size()
        print_report(
            sizes,
            as_json,
            "No jobs in the queue.",
            ["Entrypoint", "Status", "Priority", "Count"],
            lambda s: [s.entrypoint, s.status, s.priority, s.count],
        )

    asyncio_run(run())


@app.command(help="Total jobs per entrypoint and status, from the statistics table.")
def throughput(
    ctx: Context,
    period: float | None = typer.Option(
        None,
        "-p",
        "--period",
        help="Only count the last this many seconds. Default: all history.",
    ),
    as_json: JsonFlag = False,
) -> None:
    async def run() -> None:
        async with yield_queries(ctx) as q:
            totals = await q.throughput_summary(
                None if period is None else timedelta(seconds=period)
            )
        print_report(
            totals,
            as_json,
            "No processed jobs.",
            ["Entrypoint", "Status", "Total"],
            lambda t: [t.entrypoint, t.status, t.total_count],
        )

    asyncio_run(run())


@app.command(help="Show the most recent job status changes from the log table.")
def log(
    ctx: Context,
    limit: Limit = 25,
    exceptions: bool = typer.Option(
        False, "--exceptions", help="Only show entries with status 'exception'."
    ),
    as_json: JsonFlag = False,
) -> None:
    async def run() -> None:
        async with yield_queries(ctx) as q:
            if exceptions:
                entries = await InsightsService(q).exception_logs(limit)
            else:
                rows = await q.driver.fetch(q.qbq.build_queue_log_query(), limit)
                entries = [models.Log.model_validate(r) for r in rows]
        print_report(
            entries,
            as_json,
            "No log entries.",
            ["Created", "Job ID", "Entrypoint", "Status", "Priority", "Error"],
            lambda e: [
                local_time(e.created),
                e.job_id,
                e.entrypoint,
                e.status,
                e.priority,
                f"{e.traceback.exception_type}: {e.traceback.exception_message}"
                if e.traceback
                else "",
            ],
        )

    asyncio_run(run())


@app.command(
    help=(
        "List jobs in the queue table, highest priority first. "
        "With --json, payloads are reported by size only."
    )
)
def jobs(
    ctx: Context,
    limit: Limit = 25,
    offset: int = typer.Option(0, "--offset", help="Skip this many jobs first."),
    as_json: JsonFlag = False,
) -> None:
    async def run() -> None:
        async with yield_queries(ctx) as q:
            rows = await InsightsService(q).browse_queue(limit, offset)
        print_report(
            rows,
            as_json,
            "No jobs in the queue.",
            [
                "ID",
                "Entrypoint",
                "Status",
                "Priority",
                "Attempts",
                "Created",
                "Execute after",
                "Payload bytes",
            ],
            lambda j: [
                j.id,
                j.entrypoint,
                j.status,
                j.priority,
                j.attempts,
                local_time(j.created),
                local_time(j.execute_after),
                len(j.payload) if j.payload else 0,
            ],
            json_item=job_json,
        )

    asyncio_run(run())


@app.command(help="Show each PgQueuer table's persistence, estimated rows and size.")
def tables(
    ctx: Context,
    as_json: JsonFlag = False,
) -> None:
    async def run() -> None:
        async with yield_queries(ctx) as q:
            infos = await InsightsService(q).schema_info()
        print_report(
            infos,
            as_json,
            "No PgQueuer tables found.",
            ["Table", "Persistence", "Estimated rows", "Total size"],
            lambda t: [t.table_name, t.persistence, t.estimated_rows, t.total_size],
        )

    asyncio_run(run())


@app.command(help="Alter the logging durability for PgQueuer tables.")
def durability(
    ctx: Context,
    durability: sql_cmd.DurabilityArgument,
) -> None:
    """Switch durability level of PgQueuer tables without data loss."""

    async def run() -> None:
        async with yield_queries(ctx) as q:
            await q.alter_durability(durability)

    asyncio_run(run())
    typer.secho(f"Set PgQueuer durability to {durability.value}.", err=True)


@app.command(name="autovac", help="Optimize autovacuum settings for PgQueuer tables.")
def optimize_autovacuum(
    ctx: Context,
    rollback: bool = typer.Option(False, help="Reset to defaults instead."),
) -> None:
    """Apply or revert recommended autovacuum settings."""

    async def run() -> None:
        async with yield_queries(ctx) as q:
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
