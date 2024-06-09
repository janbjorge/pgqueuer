from __future__ import annotations

import argparse
import asyncio
import os
from datetime import timedelta

import asyncpg
from tabulate import tabulate, tabulate_formats

from PgQueuer.db import AsyncpgDriver, Driver
from PgQueuer.listeners import initialize_event_listener
from PgQueuer.models import LogStatistics, PGChannel
from PgQueuer.queries import DBSettings, Queries, QueryBuilder


async def display_stats(
    log_stats: list[LogStatistics],
    tablefmt: str,
) -> None:
    print(
        tabulate(
            [
                (
                    stat.created,
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
    connection: Driver,
    channel: PGChannel,
) -> None:
    listener = await initialize_event_listener(
        connection,
        channel,
    )
    while True:
        print(repr(await listener.get()))


async def fetch_and_display(
    queries: Queries,
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
        prog="PgQueuer",
    )

    common_arguments.add_argument(
        "--prefix",
        default="",
        help=(
            "All PgQueuer tables/functions/etc... will start with this prefix. "
            "(If set, addinal config is required.)"
        ),
    )

    common_arguments.add_argument(
        "--pg-dsn",
        help=(
            "Connection string in the libpq URI format, including host, port, user, "
            "database, password, passfile, and SSL options. Must be properly quoted; "
            "IPv6 addresses must be in brackets. "
            "Example: postgres://user:pass@host:port/database. Defaults to PGDSN "
            "environment variable if set."
        ),
        default=os.environ.get("PGDSN"),
    )

    common_arguments.add_argument(
        "--pg-host",
        help=(
            "Database host address, which can be an IP or domain name. "
            "Defaults to PGHOST environment variable if set."
        ),
        default=os.environ.get("PGHOST"),
    )

    common_arguments.add_argument(
        "--pg-port",
        help=(
            "Port number for the server host Defaults to PGPORT environment variable "
            "or 5432 if not set."
        ),
        default=os.environ.get("PGPORT", "5432"),
    )

    common_arguments.add_argument(
        "--pg-user",
        help=(
            "Database role for authentication. Defaults to PGUSER environment "
            "variable if set."
        ),
        default=os.environ.get("PGUSER"),
    )

    common_arguments.add_argument(
        "--pg-database",
        help=(
            "Name of the database to connect to. Defaults to PGDATABASE environment "
            "variable if set."
        ),
        default=os.environ.get("PGDATABASE"),
    )

    common_arguments.add_argument(
        "--pg-password",
        help=(
            "Password for authentication. Defaults to PGPASSWORD "
            "environment variable if set"
        ),
        default=os.environ.get("PGPASSWORD"),
    )

    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        prog="pgqueuer",
    )

    subparsers = parser.add_subparsers(
        dest="command",
        required=True,
    )

    subparsers.add_parser(
        "install",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        parents=[common_arguments],
    ).add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Prints the SQL statements that would be executed without actually "
            " applying any changes to the database."
        ),
    )

    subparsers.add_parser(
        "uninstall",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        parents=[common_arguments],
    ).add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Prints the SQL statements that would be executed without "
            "actually applying any changes to the database."
        ),
    )

    subparsers.add_parser(
        "upgrade",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        parents=[common_arguments],
    ).add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Prints the SQL statements that would be executed without "
            "actually applying any changes to the database."
        ),
    )

    dashboardparser = subparsers.add_parser(
        "dashboard",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        parents=[common_arguments],
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
        help="The number of the most recent log entries to display on the dashboard.",
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
    )
    listen_parser.add_argument(
        "--channel",
        help="Specifies the PostgreSQL NOTIFY channel to listen on for debug purposes.",
        default=DBSettings().channel,
    )

    return parser.parse_args()


async def main() -> None:
    parsed = cliparser()

    if (
        "PGQUEUER_PREFIX" not in os.environ
        and isinstance(prefix := parsed.prefix, str)
        and prefix
    ):
        os.environ["PGQUEUER_PREFIX"] = prefix

    connection = await asyncpg.connect(
        dsn=parsed.pg_dsn,
        database=parsed.pg_database,
        password=parsed.pg_password,
        port=parsed.pg_port,
        user=parsed.pg_user,
        host=parsed.pg_host,
    )
    driver = AsyncpgDriver(connection)
    queries = Queries(driver)

    match parsed.command:
        case "install":
            print(QueryBuilder().create_install_query())
            if not parsed.dry_run:
                await queries.install()
        case "uninstall":
            print(QueryBuilder().create_uninstall_query())
            if not parsed.dry_run:
                await queries.uninstall()
        case "upgrade":
            print("\n".join(QueryBuilder().create_upgrade_queries()))
            if not parsed.dry_run:
                await queries.upgrade()
        case "dashboard":
            await fetch_and_display(
                queries,
                parsed.interval,
                parsed.tail,
                parsed.table_format,
            )
        case "listen":
            await display_pg_channel(driver, PGChannel(parsed.channel))
