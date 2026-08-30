"""Startup gate comparing the code's object manifest against the installed schema."""

from __future__ import annotations

import asyncio

from typing_extensions import assert_never

from pgqueuer.core import logconfig
from pgqueuer.domain import schema_revision
from pgqueuer.ports import RepositoryPort, SupportsSchemaCheck

SCHEMA_CHECK_TIMEOUT = 30.0


def scoped(
    entries: tuple[schema_revision.SchemaObject, ...],
    tables: frozenset[str] | None,
) -> tuple[schema_revision.SchemaObject, ...]:
    """*entries* belonging to *tables*; all of them when *tables* is None."""
    if tables is None:
        return entries
    return tuple(entry for entry in entries if (entry.parent or entry.name) in tables)


async def assert_schema_usable(
    queries: RepositoryPort,
    tables: frozenset[str] | None = None,
    timeout: float = SCHEMA_CHECK_TIMEOUT,
) -> None:
    """Raise unless the installed schema can be driven by this code.

    *tables* narrows the verdict to objects hanging off those tables, so a
    manager is never held to objects it does not touch. Absent objects that
    only serve performance warn; a schema installed before markers existed is
    usable -- every object is there, only the stamps are not.
    """
    if not isinstance(queries, SupportsSchemaCheck):
        logconfig.logger.warning(
            "Repository adapter %s implements no schema_check(); skipping the schema check.",
            type(queries).__name__,
        )
        return

    try:
        result = await asyncio.wait_for(queries.schema_check(), timeout)
    except asyncio.TimeoutError:
        # Starting anyway is what turns a locked catalog into UndefinedColumn
        # errors deep in the dequeue loop.
        raise RuntimeError(
            f"The PgQueuer schema check did not complete within {timeout}s, most likely "
            "because a concurrent migration holds a catalog lock. Retry once it finishes."
        ) from None

    if isinstance(result, schema_revision.SchemaNotInstalled):
        raise RuntimeError(
            f"PgQueuer is not installed: the '{result.queue_table}' table is missing. "
            "Run 'pgq install' to set up the schema."
        )

    if isinstance(result, schema_revision.SchemaIncomplete):
        if missing := scoped(result.missing, tables):
            raise RuntimeError(
                f"The installed PgQueuer schema is missing {len(missing)} required "
                f"object(s): {', '.join(entry.label for entry in missing)}. "
                "Run 'pgq upgrade' to apply all schema changes."
            )
        return

    if isinstance(result, schema_revision.SchemaUsable):
        if degraded := scoped(result.degraded, tables):
            logconfig.logger.warning(
                "PgQueuer is running without %s performance object(s): %s. "
                "Run 'pgq upgrade' to create them.",
                len(degraded),
                ", ".join(entry.label for entry in degraded),
            )
        if result.unstamped:
            logconfig.logger.warning(
                "PgQueuer schema has no revision markers on %s object(s); it predates "
                "schema revision %s. Run 'pgq upgrade' to record them.",
                result.unstamped,
                schema_revision.SCHEMA_REVISION,
            )
        if result.ahead:
            logconfig.logger.warning(
                "PgQueuer schema is newer than this library (revision %s); %s were "
                "installed by a later version. Continuing.",
                schema_revision.SCHEMA_REVISION,
                ", ".join(result.ahead),
            )
        return

    assert_never(result)
