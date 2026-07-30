"""Offline SQL emission commands; never connect to a database."""

from __future__ import annotations

import inspect

import typer
from typing_extensions import Annotated

from pgqueuer.adapters.persistence import qb

sql_app = typer.Typer(
    help=(
        "Emit PgQueuer SQL to stdout without connecting to a database. "
        "Output is deterministic, semicolon-terminated SQL suitable for "
        "piping to psql or saving as a migration file."
    ),
    no_args_is_help=True,
)

# Shared option/argument definitions; the executing twins in cli.py reuse these
# so `pgq sql X` and `pgq X` stay flag-compatible without drifting help text.
DurabilityOption = Annotated[
    qb.Durability,
    typer.Option("--durability", "-d", help="Durability level for tables."),
]
CreateSchemaOption = Annotated[
    bool,
    typer.Option(
        "--create-schema/--no-create-schema",
        help=(
            "Include CREATE SCHEMA IF NOT EXISTS when --schema is set. Disable when "
            "the role lacks CREATE on the database and the schema already exists."
        ),
    ),
]
WidenIdOption = Annotated[
    bool,
    typer.Option(
        "--widen-id/--no-widen-id",
        help=(
            "Widen legacy int4 id columns and sequences to BIGINT. Takes an "
            "ACCESS EXCLUSIVE lock and rewrites each table; use --no-widen-id "
            "to skip and apply the widening out-of-band."
        ),
    ),
]
DurabilityArgument = Annotated[
    qb.Durability,
    typer.Argument(
        help=(
            "The durability mode to set: 'volatile' (all unlogged), "
            "'balanced' (main table logged, others unlogged), 'durable' (all logged)."
        ),
    ),
]


def resolve_settings(ctx: typer.Context) -> qb.DBSettings:
    """The single DBSettings built by the root callback (``AppConfig``)."""
    settings: qb.DBSettings = ctx.obj.settings
    return settings


def render_install(settings: qb.DBSettings, create_schema: bool) -> str:
    qbe = qb.QueryBuilderEnvironment(settings)
    return inspect.cleandoc(qbe.build_install_query(create_schema=create_schema)).strip()


def render_uninstall(settings: qb.DBSettings) -> str:
    return inspect.cleandoc(qb.QueryBuilderEnvironment(settings).build_uninstall_query()).strip()


def render_upgrade(settings: qb.DBSettings) -> str:
    qbe = qb.QueryBuilderEnvironment(settings)
    return "\n\n".join(
        inspect.cleandoc(statement).strip() for statement in qbe.build_upgrade_queries()
    )


def render_durability(settings: qb.DBSettings) -> str:
    qbe = qb.QueryBuilderEnvironment(settings)
    return "\n\n".join(
        inspect.cleandoc(statement).strip() for statement in qbe.build_alter_durability_query()
    )


def render_autovac(settings: qb.DBSettings, rollback: bool) -> str:
    qbe = qb.QueryBuilderEnvironment(settings)
    query = (
        qbe.build_optimize_autovacuum_rollback_query()
        if rollback
        else qbe.build_optimize_autovacuum_query()
    )
    return inspect.cleandoc(query).strip()


@sql_app.command(help="SQL to create the PgQueuer schema.")
def install(
    ctx: typer.Context,
    durability: DurabilityOption = qb.Durability.durable,
    create_schema: CreateSchemaOption = True,
) -> None:
    settings = resolve_settings(ctx).model_copy(update={"durability": durability})
    typer.echo(render_install(settings, create_schema))


@sql_app.command(help="SQL to drop all PgQueuer objects.")
def uninstall(ctx: typer.Context) -> None:
    typer.echo(render_uninstall(resolve_settings(ctx)))


@sql_app.command(help="SQL to migrate an existing installation to the current version.")
def upgrade(
    ctx: typer.Context,
    durability: DurabilityOption = qb.Durability.durable,
    widen_id: WidenIdOption = True,
) -> None:
    settings = resolve_settings(ctx).model_copy(
        update={"durability": durability, "widen_id": widen_id}
    )
    typer.echo(render_upgrade(settings))


@sql_app.command(help="SQL to switch table durability without data loss.")
def durability(
    ctx: typer.Context,
    durability: DurabilityArgument,
) -> None:
    typer.echo(
        render_durability(resolve_settings(ctx).model_copy(update={"durability": durability}))
    )


@sql_app.command(help="SQL for recommended autovacuum settings (--rollback to reset).")
def autovac(
    ctx: typer.Context,
    rollback: bool = typer.Option(False, help="Reset to defaults instead."),
) -> None:
    typer.echo(render_autovac(resolve_settings(ctx), rollback))
