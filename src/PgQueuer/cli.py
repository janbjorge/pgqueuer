import argparse
import asyncio
import os
from datetime import timedelta

import asyncpg
from tabulate import tabulate, tabulate_formats

from PgQueuer.models import LogStatistics
from PgQueuer.queries import Queries


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


async def fetch_and_dispay(
    queries: Queries,
    interval: timedelta | None,
    tail: int,
    tablefmt: str,
) -> None:
    clear_and_home = "\033[2J\033[H"
    while True:
        print(clear_and_home, end="")
        stats = await queries.log_statistics(tail)  # Fetch stats once and reuse
        await display_stats(stats, tablefmt)
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
        prog="pgcachewatch",
    )

    subparsers = parser.add_subparsers(
        dest="command",
        required=True,
    )

    subparsers.add_parser(
        "install",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        parents=[common_arguments],
    )

    subparsers.add_parser(
        "uninstall",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        parents=[common_arguments],
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

    return parser.parse_args()


async def main() -> None:
    parsed = cliparser()

    if (
        "PGQUEUER_PREFIX" not in os.environ
        and isinstance(prefix := parsed.prefix, str)
        and prefix
    ):
        os.environ["PGQUEUER_PREFIX"] = prefix

    async with asyncpg.create_pool(
        parsed.pg_dsn,
        database=parsed.pg_database,
        password=parsed.pg_password,
        port=parsed.pg_port,
        user=parsed.pg_user,
        host=parsed.pg_host,
        max_size=1,
        min_size=0,
    ) as pool:
        queries = Queries(pool)
        match parsed.command:
            case "install":
                await queries.install()
            case "uninstall":
                await queries.uninstall()
            case "dashboard":
                await fetch_and_dispay(
                    queries,
                    parsed.interval,
                    parsed.tail,
                    parsed.table_format,
                )
