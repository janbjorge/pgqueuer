from __future__ import annotations

import pytest

from pgqueuer import db, queries
from pgqueuer.adapters.cli import sql_cmd
from pgqueuer.adapters.persistence import qb
from pgqueuer.core import schema_check
from pgqueuer.domain import schema_revision
from pgqueuer.domain.settings import DBSettings

from .helpers import queries_for


def strip_markers(qbe: qb.QueryBuilderEnvironment) -> str:
    """Undo every marker, reproducing an install that predates them."""
    return "\n".join(
        statement.replace(
            qbe.render_marker_literal(schema_revision.SCHEMA_REVISION),
            "NULL",
        )
        for statement in qbe.build_schema_marker_statements()
    )


def revision_1_manifest() -> frozenset[str]:
    """Labels shipped at SCHEMA_REVISION 1. Frozen: a later revision may only add."""
    return frozenset(
        {
            *(
                f"column 'pgqueuer.{column}'"
                for column in (
                    "attempts",
                    "created",
                    "dedupe_key",
                    "entrypoint",
                    "execute_after",
                    "headers",
                    "heartbeat",
                    "id",
                    "payload",
                    "priority",
                    "queue_manager_id",
                    "status",
                    "updated",
                )
            ),
            *(
                f"column 'pgqueuer_log.{column}'"
                for column in (
                    "aggregated",
                    "created",
                    "entrypoint",
                    "id",
                    "job_id",
                    "priority",
                    "status",
                    "traceback",
                )
            ),
            *(
                f"column 'pgqueuer_schedules.{column}'"
                for column in (
                    "created",
                    "entrypoint",
                    "expression",
                    "heartbeat",
                    "id",
                    "last_run",
                    "next_run",
                    "status",
                    "updated",
                )
            ),
            *(
                f"column 'pgqueuer_statistics.{column}'"
                for column in ("count", "created", "entrypoint", "id", "priority", "status")
            ),
            *(
                f"enum_value 'pgqueuer_status.{value}'"
                for value in (
                    "canceled",
                    "deleted",
                    "exception",
                    "failed",
                    "picked",
                    "queued",
                    "successful",
                )
            ),
            *(
                f"index '{index}'"
                for index in (
                    "pgqueuer_ep_ea_idx",
                    "pgqueuer_ep_prio_id_idx",
                    "pgqueuer_heartbeat_id_id1_idx",
                    "pgqueuer_log_created",
                    "pgqueuer_log_job_id_status",
                    "pgqueuer_log_not_aggregated",
                    "pgqueuer_log_status",
                    "pgqueuer_priority_id_id1_idx",
                    "pgqueuer_queue_manager_id_idx",
                    "pgqueuer_statistics_unique_count",
                    "pgqueuer_unique_dedupe_key",
                    "pgqueuer_updated_id_id1_idx",
                )
            ),
            "function 'fn_pgqueuer_changed'",
            "table 'pgqueuer'",
            "table 'pgqueuer_log'",
            "table 'pgqueuer_schedules'",
            "table 'pgqueuer_statistics'",
            "trigger 'pgqueuer.tg_pgqueuer_changed'",
            "type 'pgqueuer_status'",
        }
    )


def test_manifest_only_ever_grows() -> None:
    """CONTRIBUTING invariant 1: a revision bump may add objects, never drop one.

    Dropping an entry is what silently waves through a worker whose code still
    needs the object. Removing one here is a major release, and updating this
    baseline is the deliberate act that says so.
    """
    current = {entry.label for entry in schema_revision.manifest(DBSettings())}
    assert revision_1_manifest() <= current


async def test_install_stamps_every_manifest_object(apgdriver: db.Driver) -> None:
    """The guard that makes the manifest trustworthy: DDL and manifest cannot drift."""
    q = queries.Queries(apgdriver)
    await q.uninstall()
    await q.install()

    result = await q.schema_check()
    assert isinstance(result, schema_revision.SchemaUsable)
    assert result.unstamped == 0
    assert result.ahead == ()


async def test_emitted_markers_are_plain_declarative_sql() -> None:
    qbe = qb.QueryBuilderEnvironment(DBSettings())
    statements = list(qbe.build_schema_marker_statements())

    assert statements
    for statement in statements:
        assert statement.startswith("COMMENT ON ")
        assert "DO " not in statement
        assert "EXECUTE" not in statement


async def test_install_and_emitted_sql_agree_on_markers() -> None:
    """`pgq install` and `pgq sql install | psql` must stamp identically."""
    settings = DBSettings()
    qbe = qb.QueryBuilderEnvironment(settings)

    def comment_lines(sql: str) -> set[str]:
        return {line.strip() for line in sql.splitlines() if line.strip().startswith("COMMENT ON")}

    executed = comment_lines(qbe.build_install_query())
    emitted = comment_lines(sql_cmd.render_install(settings, create_schema=True))

    assert executed == emitted
    assert executed == set(qbe.build_schema_marker_statements())


async def test_pre_marker_install_is_usable_and_warns(apgdriver: db.Driver) -> None:
    """A schema installed before markers existed keeps working; it is not a break."""
    q = queries.Queries(apgdriver)
    await apgdriver.execute(strip_markers(q.qbe))

    result = await q.schema_check()
    assert isinstance(result, schema_revision.SchemaUsable)
    assert result.unstamped > 0

    # Warns, does not raise.
    await schema_check.assert_schema_usable(q)


async def test_upgrade_stamps_a_pre_marker_install(apgdriver: db.Driver) -> None:
    """Self-healing: upgrade records the revision even when no DDL was needed."""
    q = queries.Queries(apgdriver)
    await apgdriver.execute(strip_markers(q.qbe))
    assert isinstance(await q.schema_check(), schema_revision.SchemaUsable)

    await q.upgrade()

    result = await q.schema_check()
    assert isinstance(result, schema_revision.SchemaUsable)
    assert result.unstamped == 0


async def test_missing_object_is_named(apgdriver: db.Driver) -> None:
    """Existence checking does not depend on stamps, and points at the culprit."""
    q = queries.Queries(apgdriver)
    index = f"{q.qbe.settings.queue_table}_ep_ea_idx"
    await apgdriver.execute(f"DROP INDEX {index};")

    result = await q.schema_check()
    assert isinstance(result, schema_revision.SchemaIncomplete)
    assert [entry.name for entry in result.missing] == [index]

    with pytest.raises(RuntimeError, match=index):
        await schema_check.assert_schema_usable(q)


async def test_missing_column_is_named_on_a_pre_marker_install(apgdriver: db.Driver) -> None:
    q = queries.Queries(apgdriver)
    await apgdriver.execute(strip_markers(q.qbe))
    await apgdriver.execute(f"ALTER TABLE {q.qbe.qualified.queue_table} DROP COLUMN attempts;")

    result = await q.schema_check()
    assert isinstance(result, schema_revision.SchemaIncomplete)
    assert [entry.name for entry in result.missing] == ["attempts"]


async def test_not_installed_is_distinct_from_incomplete(apgdriver: db.Driver) -> None:
    q = queries.Queries(apgdriver)
    await q.uninstall()

    result = await q.schema_check()
    assert isinstance(result, schema_revision.SchemaNotInstalled)

    with pytest.raises(RuntimeError, match="pgq install"):
        await schema_check.assert_schema_usable(q)


async def test_human_written_comment_does_not_raise(apgdriver: db.Driver) -> None:
    q = queries.Queries(apgdriver)
    await apgdriver.execute(
        f"COMMENT ON TABLE {q.qbe.qualified.queue_table} IS 'notes about this table';"
    )

    result = await q.schema_check()
    assert isinstance(result, schema_revision.SchemaUsable)
    assert result.unstamped == 1
    await schema_check.assert_schema_usable(q)


async def test_schema_ahead_of_code_warns_but_stays_usable(apgdriver: db.Driver) -> None:
    """Rolling deploys upgrade the schema first; an older worker must still run."""
    q = queries.Queries(apgdriver)
    ahead = q.qbe.render_marker_literal(schema_revision.SCHEMA_REVISION + 5)
    await apgdriver.execute(f"COMMENT ON TABLE {q.qbe.qualified.queue_table} IS {ahead};")

    result = await q.schema_check()
    assert isinstance(result, schema_revision.SchemaUsable)
    assert result.ahead == (q.qbe.settings.queue_table,)
    await schema_check.assert_schema_usable(q)


async def test_markers_are_written_under_schema_and_prefix(apgdriver: db.Driver) -> None:
    await queries.Queries(apgdriver).uninstall()
    settings = DBSettings(db_schema="marker_iso", prefix="mrk_")
    q = queries_for(apgdriver, settings)
    await q.install()

    try:
        result = await q.schema_check()
        assert isinstance(result, schema_revision.SchemaUsable)
        assert result.unstamped == 0
    finally:
        await q.uninstall()
        await apgdriver.execute("DROP SCHEMA IF EXISTS marker_iso CASCADE;")


async def test_repeated_upgrades_are_idempotent(apgdriver: db.Driver) -> None:
    """Re-stamping must be a no-op; upgrade is replayed on every deploy."""
    q = queries.Queries(apgdriver)
    await q.upgrade()
    await q.upgrade()

    result = await q.schema_check()
    assert isinstance(result, schema_revision.SchemaUsable)
    assert result.unstamped == 0
    assert result.ahead == ()


@pytest.mark.parametrize(
    "comment",
    [
        None,
        "",
        "not json",
        "[]",
        '{"other": {"schema_revision": 3}}',
        '{"pgqueuer": []}',
        '{"pgqueuer": {}}',
        '{"pgqueuer": {"schema_revision": "3"}}',
        '{"pgqueuer": {"schema_revision": true}}',
        '{"pgqueuer": {"schema_revision": null}}',
    ],
)
async def test_parse_marker_never_raises(comment: str | None) -> None:
    assert schema_revision.parse_marker(comment) is None


async def test_parse_marker_reads_a_revision() -> None:
    assert schema_revision.parse_marker('{"pgqueuer": {"schema_revision": 7}}') == 7


async def test_marker_literal_rejects_unquotable_payload() -> None:
    qbe = qb.QueryBuilderEnvironment(DBSettings())
    assert qbe.render_marker_literal(1) == '\'{"pgqueuer":{"schema_revision":1}}\''
