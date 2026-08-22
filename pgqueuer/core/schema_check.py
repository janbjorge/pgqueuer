"""Startup gate comparing the code's object manifest against the installed schema."""

from __future__ import annotations

import asyncio

from typing_extensions import assert_never

from pgqueuer.core import logconfig
from pgqueuer.domain import schema_revision
from pgqueuer.ports import RepositoryPort

# A check that never returns is worse than one that never ran: a manager stuck
# behind a catalog lock would hang forever instead of starting.
SCHEMA_CHECK_TIMEOUT = 30.0


async def assert_schema_usable(
    queries: RepositoryPort,
    timeout: float = SCHEMA_CHECK_TIMEOUT,
) -> None:
    """Raise unless the installed schema can be driven by this code.

    Missing objects are fatal and named. A schema installed before markers
    existed is usable -- every object is there, only the stamps are not -- so
    it warns and proceeds rather than forcing an upgrade on users who take a
    minor release.
    """
    try:
        result = await asyncio.wait_for(queries.schema_check(), timeout)
    except asyncio.TimeoutError:
        logconfig.logger.warning(
            "Schema revision check timed out after %ss; starting without it.",
            timeout,
        )
        return

    if isinstance(result, schema_revision.SchemaNotInstalled):
        raise RuntimeError(
            f"PgQueuer is not installed: the '{result.queue_table}' table is missing. "
            "Run 'pgq install' to set up the schema."
        )

    if isinstance(result, schema_revision.SchemaIncomplete):
        missing = ", ".join(entry.label for entry in result.missing)
        raise RuntimeError(
            f"The installed PgQueuer schema is missing {len(result.missing)} required "
            f"object(s): {missing}. Run 'pgq upgrade' to apply all schema changes."
        )

    if isinstance(result, schema_revision.SchemaUsable):
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
