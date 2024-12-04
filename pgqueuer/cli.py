from __future__ import annotations

import argparse
import asyncio
import contextlib
import os
from datetime import timedelta

from tabulate import tabulate

from . import db, listeners, models, qb, queries, supervisor


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
        print(
            repr(
                (await queue.get()).root,
            ),
        )


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
    queries: queries.Queries,
    interval: timedelta | None,
    tail: int,
) -> None:
    clear_and_home = "\033[2J\033[H"
    while True:
        print(clear_and_home, end="")
        await display_stats(await queries.log_statistics(tail))
        if interval is None:
            return
        await asyncio.sleep(interval.total_seconds())


def cliparser() -> argparse.Namespace:
    common_arguments = argparse.ArgumentParser(
        add_help=False,
        prog="pgqueuer",
    )

    common_arguments.add_argument(
        "--prefix",
        default="",
        help=(
            "Specify a prefix for all pgqueuer tables, functions, etc. "
            "This helps in avoiding conflicts if running multiple instances. "
            "(If set, additional config is required.)"
        ),
    )

    common_arguments.add_argument(
        "--pg-dsn",
        help=(
            "Provide the connection string in the libpq URI format, including host, port, user, "
            "database, password, passfile, and SSL options. Must be properly quoted; "
            "IPv6 addresses must be in brackets. Example: postgres://user:pass@host:port/database. "
            "Defaults to the PGDSN environment variable if set."
        ),
        default=os.environ.get("PGDSN"),
    )

    common_arguments.add_argument(
        "--pg-host",
        help=(
            "Specify the database host address, which can be an IP or domain name. "
            "Defaults to the PGHOST environment variable if set."
        ),
        default=os.environ.get("PGHOST"),
    )

    common_arguments.add_argument(
        "--pg-port",
        help=(
            "Specify the port number for the server host. "
            "Defaults to the PGPORT environment variable or 5432 if not set."
        ),
        default=os.environ.get("PGPORT", "5432"),
    )

    common_arguments.add_argument(
        "--pg-user",
        help=(
            "Specify the database role for authentication. "
            "Defaults to the PGUSER environment variable if set."
        ),
        default=os.environ.get("PGUSER"),
    )

    common_arguments.add_argument(
        "--pg-database",
        help=(
            "Specify the name of the database to connect to. "
            "Defaults to the PGDATABASE environment variable if set."
        ),
        default=os.environ.get("PGDATABASE"),
    )

    common_arguments.add_argument(
        "--pg-password",
        help=(
            "Specify the password for authentication. "
            "Defaults to the PGPASSWORD environment variable if set."
        ),
        default=os.environ.get("PGPASSWORD"),
    )

    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        prog="pgqueuer",
        description=(
            "PGQueuer command-line interface for managing the "
            "PostgreSQL-backed job queue system."
        ),
    )

    subparsers = parser.add_subparsers(
        dest="command", required=True, help="Available commands for managing pgqueuer."
    )

    subparsers.add_parser(
        "install",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        parents=[common_arguments],
        help="Install the necessary database schema for PGQueuer.",
    ).add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Print the SQL statements that would be executed without actually "
            "applying any changes to the database."
        ),
    )

    subparsers.add_parser(
        "uninstall",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        parents=[common_arguments],
        help="Remove the PGQueuer schema from the database.",
    ).add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Print the SQL statements that would be executed without "
            "actually applying any changes to the database."
        ),
    )

    subparsers.add_parser(
        "upgrade",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        parents=[common_arguments],
        help="Apply upgrades to the existing PGQueuer database schema.",
    ).add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Print the SQL statements that would be executed without "
            "actually applying any changes to the database."
        ),
    )

    dashboardparser = subparsers.add_parser(
        "dashboard",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        parents=[common_arguments],
        help="Display a live dashboard showing job statistics.",
    )
    dashboardparser.add_argument(
        "-i",
        "--interval",
        help="Set the refresh interval in seconds for updating the dashboard display.",
        default=None,
        type=lambda x: timedelta(seconds=float(x)),
    )
    dashboardparser.add_argument(
        "-n",
        "--tail",
        help="Specify the number of the most recent log entries to display on the dashboard.",
        type=int,
        default=25,
    )

    listen_parser = subparsers.add_parser(
        "listen",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        parents=[common_arguments],
        help="Listen to a PostgreSQL NOTIFY channel for debug purposes.",
    )
    listen_parser.add_argument(
        "--channel",
        help="Specify the PostgreSQL NOTIFY channel to listen on for debug purposes.",
        default=qb.DBSettings().channel,
    )

    wm_parser = subparsers.add_parser(
        "run",
        description="Run a QueueManager from a factory function.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        parents=[common_arguments],
        help="Start a QueueManager to manage job queues and process jobs.",
    )
    wm_parser.add_argument(
        "--dequeue-timeout",
        help=(
            "Specify the timeout (in seconds) to wait to dequeue jobs. "
            "This determines how long the QueueManager will wait for new jobs before retrying."
        ),
        type=lambda s: timedelta(seconds=float(s)),
        default=timedelta(seconds=30.0),
    )
    wm_parser.add_argument(
        "--batch-size",
        help=(
            "Specify the number of jobs to dequeue in each batch. "
            "A larger batch size can increase throughput but may require more memory."
        ),
        type=int,
        default=10,
    )
    wm_parser.add_argument(
        "factory_fn",
        help=(
            "Path to the QueueManager or Schedule factory function, "
            "e.g., 'myapp.create_queue_manager'."
        ),
    )

    schedule_parser = subparsers.add_parser(
        "schedules",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        parents=[common_arguments],
        help="Manage schedules in the PGQueuer system.",
    )
    schedule_parser.add_argument(
        "-r",
        "--remove",
        help=(
            "Remove specified schedule from the database. "
            "If no arguments are provided, displays the contents of the schedule table."
        ),
        nargs="*",
        default=[],
    )

    return parser.parse_args()


async def query_adapter(conninfo: str) -> queries.Queries:
    with contextlib.suppress(ImportError):
        import asyncpg

        return queries.Queries(
            db.AsyncpgDriver(
                await asyncpg.connect(
                    dsn=conninfo,
                )
            )
        )

    with contextlib.suppress(ImportError):
        import psycopg

        return queries.Queries(
            db.PsycopgDriver(
                await psycopg.AsyncConnection.connect(
                    conninfo=conninfo,
                    autocommit=True,
                )
            )
        )

    raise RuntimeError(
        "Neither asyncpg nor psycopg could be imported. Install "
        "either 'asyncpg' or 'psycopg' to use the querier function."
    )


async def main() -> None:  # noqa: C901
    parsed = cliparser()

    if "PGQUEUER_PREFIX" not in os.environ and isinstance(prefix := parsed.prefix, str) and prefix:
        os.environ["PGQUEUER_PREFIX"] = prefix

    dsn = parsed.pg_dsn or db.dsn(
        database=parsed.pg_database,
        password=parsed.pg_password,
        port=parsed.pg_port,
        user=parsed.pg_user,
        host=parsed.pg_host,
    )

    match parsed.command:
        case "install":
            print(queries.qb.QueryBuilderEnvironment().create_install_query())
            if not parsed.dry_run:
                await (await query_adapter(dsn)).install()
        case "uninstall":
            print(queries.qb.QueryBuilderEnvironment().create_uninstall_query())
            if not parsed.dry_run:
                await (await query_adapter(dsn)).uninstall()
        case "upgrade":
            print(
                f"\n{'-'*50}\n".join(queries.qb.QueryBuilderEnvironment().create_upgrade_queries())
            )
            if not parsed.dry_run:
                await (await query_adapter(dsn)).upgrade()
        case "dashboard":
            await fetch_and_display(
                await query_adapter(dsn),
                parsed.interval,
                parsed.tail,
            )
        case "listen":
            await display_pg_channel(
                (await query_adapter(dsn)).driver,
                models.PGChannel(parsed.channel),
            )
        case "run":
            await supervisor.runit(
                parsed.factory_fn,
                dequeue_timeout=parsed.dequeue_timeout,
                batch_size=parsed.batch_size,
            )
        case "schedules":
            if parsed.remove:
                await (await query_adapter(dsn)).delete_schedule(
                    {models.ScheduleId(int(x)) for x in parsed.remove if x.isdigit()},
                    {str(x) for x in parsed.remove if not x.isdigit()},
                )
            await display_schedule(
                await (await query_adapter(dsn)).peak_schedule(),
            )
