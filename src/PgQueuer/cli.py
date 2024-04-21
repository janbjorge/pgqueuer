import argparse
import os

import asyncpg

from PgQueuer import queries


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
        min_size=0,
        max_size=1,
    ) as pool:
        match parsed.command:
            case "install":
                await queries.InstallUninstallQueries(pool=pool).install()
            case "uninstall":
                await queries.InstallUninstallQueries(pool=pool).uninstall()
