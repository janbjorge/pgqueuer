from __future__ import annotations

import argparse
import asyncio
import os
from datetime import timedelta
from typing import Literal

from tabulate import tabulate, tabulate_formats

from . import db, listeners, models, queries, supervisor


async def display_stats(
    log_stats: list[models.LogStatistics],
    tablefmt: str,
) -> None:
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
            tablefmt=tablefmt,
        )
    )


async def display_pg_channel(
    connection: db.Driver,
    channel: models.PGChannel,
) -> None:
    listener = await listeners.initialize_notice_event_listener(
        connection,
        channel,
        {},
        {},
    )
    while True:
        print(repr(await listener.get()))


async def fetch_and_display(
    queries: queries.Queries,
    interval: timedelta | None,
    tail: int,
    tablefmt: str,
) -> None:
    clear_and_home = "\033[2J\033[H"
    while True:
        print(clear_and_home, end="")
        await display_stats(await queries.log_statistics(tail), tablefmt)
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
        "--driver",
        default="apg",
        help="Specify the PostgreSQL driver to be used: asyncpg (apg) or psycopg (psy).",
        choices=["apg", "psy"],
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
    dashboardparser.add_argument(
        "--table-format",
        default="pretty",
        help=(
            "Specify the format of the table used to display statistics. "
            "Options are provided by the tabulate library."
        ),
        choices=tabulate_formats,
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
        default=queries.DBSettings().channel,
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
        "--retry-timer",
        help=(
            "Specify the time to wait (in seconds) before retrying jobs "
            "that have been picked but not processed."
        ),
        type=lambda s: timedelta(seconds=float(s)),
    )
    wm_parser.add_argument(
        "qm_factory",
        help="Path to the QueueManager factory function, e.g., 'myapp.create_queue_manager'.",
    )

    return parser.parse_args()


async def querier(
    driver: Literal["psy", "apg", "apgpool"],
    conninfo: str,
) -> queries.Queries:
    match driver:
        case "apg":
            import asyncpg

            return queries.Queries(
                db.AsyncpgDriver(
                    await asyncpg.connect(dsn=conninfo),
                )
            )
        case "apgpool":
            import asyncpg

            pool = await asyncpg.create_pool(dsn=conninfo)
            assert pool is not None
            return queries.Queries(db.AsyncpgPoolDriver(pool))

        case "psy":
            import psycopg

            return queries.Queries(
                db.PsycopgDriver(
                    await psycopg.AsyncConnection.connect(
                        conninfo=conninfo,
                        autocommit=True,
                    )
                )
            )

    raise NotImplementedError(driver)


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
            print(queries.QueryBuilder().create_install_query())
            if not parsed.dry_run:
                await (await querier(parsed.driver, dsn)).install()
        case "uninstall":
            print(queries.QueryBuilder().create_uninstall_query())
            if not parsed.dry_run:
                await (await querier(parsed.driver, dsn)).uninstall()
        case "upgrade":
            print(f"\n{'-'*50}\n".join(queries.QueryBuilder().create_upgrade_queries()))
            if not parsed.dry_run:
                await (await querier(parsed.driver, dsn)).upgrade()
        case "dashboard":
            await fetch_and_display(
                await querier(parsed.driver, dsn),
                parsed.interval,
                parsed.tail,
                parsed.table_format,
            )
        case "listen":
            await display_pg_channel(
                (await querier(parsed.driver, dsn)).driver,
                models.PGChannel(parsed.channel),
            )
        case "run":
            await supervisor.runit(
                parsed.qm_factory,
                dequeue_timeout=parsed.dequeue_timeout,
                batch_size=parsed.batch_size,
                retry_timer=parsed.retry_timer,
            )
