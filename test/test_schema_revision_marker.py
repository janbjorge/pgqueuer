from __future__ import annotations

import asyncio

import pytest

from pgqueuer import db, queries
from pgqueuer.adapters.cli import sql_cmd
from pgqueuer.adapters.persistence import qb
from pgqueuer.core import schema_check
from pgqueuer.domain import schema_revision
from pgqueuer.domain.settings import DBSettings

from .helpers import queries_for

INSTALLED_OBJECTS_QUERY = """WITH ns AS (
    SELECT oid FROM pg_namespace WHERE nspname = current_schema()
)
SELECT 'table' AS kind, relation.relname AS name, NULL::name AS parent
FROM pg_class relation JOIN ns ON relation.relnamespace = ns.oid
WHERE relation.relkind IN ('r', 'p', 'v', 'm', 'f')
UNION ALL
SELECT 'column', attribute.attname, relation.relname
FROM pg_class relation
JOIN ns ON relation.relnamespace = ns.oid
JOIN pg_attribute attribute ON attribute.attrelid = relation.oid
    AND attribute.attnum > 0 AND NOT attribute.attisdropped
WHERE relation.relkind IN ('r', 'p', 'v', 'm', 'f')
UNION ALL
SELECT 'index', relation.relname, parent.relname
FROM pg_class relation
JOIN ns ON relation.relnamespace = ns.oid
JOIN pg_index ON pg_index.indexrelid = relation.oid
JOIN pg_class parent ON parent.oid = pg_index.indrelid
WHERE relation.relkind IN ('i', 'I')
  AND NOT pg_index.indisprimary
  AND NOT EXISTS (SELECT FROM pg_constraint WHERE conindid = relation.oid)
UNION ALL
SELECT 'type', enum_type.typname, NULL
FROM pg_type enum_type JOIN ns ON enum_type.typnamespace = ns.oid
WHERE enum_type.typtype = 'e'
UNION ALL
SELECT 'enum_value', enum_label.enumlabel, enum_type.typname
FROM pg_enum enum_label
JOIN pg_type enum_type ON enum_type.oid = enum_label.enumtypid
JOIN ns ON enum_type.typnamespace = ns.oid
UNION ALL
SELECT 'function', routine.proname, NULL
FROM pg_proc routine JOIN ns ON routine.pronamespace = ns.oid
UNION ALL
SELECT 'trigger', trigger_row.tgname, relation.relname
FROM pg_trigger trigger_row
JOIN pg_class relation ON relation.oid = trigger_row.tgrelid
JOIN ns ON relation.relnamespace = ns.oid
WHERE NOT trigger_row.tgisinternal;"""


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
    index = f"{q.qbe.settings.queue_table}_unique_dedupe_key"
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


async def test_parse_marker_survives_deeply_nested_json() -> None:
    """json.loads blows the C stack on nesting, which is not a ValueError."""
    assert schema_revision.parse_marker("[" * 100_000) is None


async def test_install_creates_nothing_outside_manifest(apgdriver: db.Driver) -> None:
    """The drift guard in the direction a contributor actually forgets."""
    q = queries.Queries(apgdriver)
    await q.uninstall()
    await q.install()

    manifest = {entry.key for entry in q.qbe.schema_manifest()}
    installed = await apgdriver.fetch(INSTALLED_OBJECTS_QUERY)
    unlisted = [
        f"{row['kind']} {row['name']}"
        for row in installed
        if (row["kind"], row["name"], row["parent"]) not in manifest
    ]

    assert unlisted == []


async def test_absent_performance_index_warns_but_starts(apgdriver: db.Driver) -> None:
    """Released `pgq install` predates some indexes; that is a warning, not a crash."""
    q = queries.Queries(apgdriver)
    index = f"{q.qbe.settings.queue_table}_heartbeat_id_id1_idx"
    await apgdriver.execute(f"DROP INDEX {index};")

    result = await q.schema_check()
    assert isinstance(result, schema_revision.SchemaUsable)
    assert [entry.name for entry in result.degraded] == [index]

    await schema_check.assert_schema_usable(q)


async def test_absent_conflict_arbiter_index_is_fatal(apgdriver: db.Driver) -> None:
    """The unique indexes arbitrate ON CONFLICT; without them enqueue fails outright."""
    q = queries.Queries(apgdriver)
    index = f"{q.qbe.settings.statistics_table}_unique_count"
    await apgdriver.execute(f"DROP INDEX {index};")

    assert isinstance(await q.schema_check(), schema_revision.SchemaIncomplete)
    with pytest.raises(RuntimeError, match=index):
        await schema_check.assert_schema_usable(q)


async def test_upgrade_recreates_a_dropped_index(apgdriver: db.Driver) -> None:
    """`pgq upgrade` is the advice the check gives; it has to actually heal."""
    q = queries.Queries(apgdriver)
    await apgdriver.execute(f"DROP INDEX {q.qbe.settings.queue_table}_priority_id_id1_idx;")
    await apgdriver.execute(f"DROP INDEX {q.qbe.settings.statistics_table}_unique_count;")

    await q.upgrade()

    result = await q.schema_check()
    assert isinstance(result, schema_revision.SchemaUsable)
    assert result.degraded == ()
    assert result.unstamped == 0


async def test_upgrade_survives_an_object_it_cannot_create(apgdriver: db.Driver) -> None:
    """A COMMENT on an absent object must not abort the run and strand the operator."""
    q = queries.Queries(apgdriver)
    trigger = q.qbe.settings.trigger
    await apgdriver.execute(f"DROP TRIGGER {trigger} ON {q.qbe.qualified.queue_table};")

    await q.upgrade()

    result = await q.schema_check()
    assert isinstance(result, schema_revision.SchemaIncomplete)
    assert [entry.name for entry in result.missing] == [trigger]


async def test_a_dropped_table_is_reported_once(apgdriver: db.Driver) -> None:
    """Dependents of an absent object explain nothing; only the root cause is named."""
    q = queries.Queries(apgdriver)
    table = q.qbe.settings.schedules_table
    await apgdriver.execute(f"DROP TABLE {q.qbe.qualified.schedules_table};")

    result = await q.schema_check()
    assert isinstance(result, schema_revision.SchemaIncomplete)
    assert [entry.label for entry in result.missing] == [f"table '{table}'"]


async def test_scheduler_scope_ignores_queue_side_objects(apgdriver: db.Driver) -> None:
    """A scheduler-only process must not crashloop on drift it never touches."""
    q = queries.Queries(apgdriver)
    settings = q.qbe.settings
    await apgdriver.execute(f"DROP INDEX {settings.statistics_table}_unique_count;")

    with pytest.raises(RuntimeError):
        await schema_check.assert_schema_usable(q)

    await schema_check.assert_schema_usable(
        q,
        tables=frozenset({settings.schedules_table, settings.queue_table_log}),
    )


async def test_schema_check_timeout_is_fatal() -> None:
    """Starting anyway turns a locked catalog into UndefinedColumn deep in the loop."""

    class HangingQueries:
        async def schema_check(self) -> schema_revision.SchemaCheck:
            await asyncio.sleep(30)
            raise AssertionError("unreachable")

    with pytest.raises(RuntimeError, match="did not complete"):
        await schema_check.assert_schema_usable(HangingQueries(), timeout=0.01)  # type: ignore[arg-type]


async def test_adapter_without_schema_check_is_skipped() -> None:
    """Adapters written against the 1.x port predate schema_check."""

    class LegacyAdapter:
        pass

    await schema_check.assert_schema_usable(LegacyAdapter())  # type: ignore[arg-type]


async def test_long_prefix_names_survive_identifier_truncation(apgdriver: db.Driver) -> None:
    """Postgres clips names at 63 bytes; the manifest has to compare on what it stored."""
    await queries.Queries(apgdriver).uninstall()
    settings = DBSettings(prefix="a_very_long_pgqueuer_deployment_prefix_")
    q = queries_for(apgdriver, settings)
    await q.install()

    try:
        assert any(
            len(entry.name.encode()) == schema_revision.POSTGRES_IDENTIFIER_LIMIT
            for entry in q.qbe.schema_manifest()
        )
        result = await q.schema_check()
        assert isinstance(result, schema_revision.SchemaUsable)
        assert result.degraded == ()
        assert result.unstamped == 0
    finally:
        await q.uninstall()


async def test_markers_query_sees_partitioned_indexes(apgdriver: db.Driver) -> None:
    """A repartitioned table carries relkind 'I' indexes; they are not missing."""
    qbe = qb.QueryBuilderEnvironment(DBSettings())
    await apgdriver.execute(
        """CREATE TABLE partitioned_queue (id BIGINT, created TIMESTAMPTZ)
            PARTITION BY RANGE (created);
        CREATE INDEX partitioned_queue_created_idx ON partitioned_queue (created);"""
    )

    rows = await apgdriver.fetch(
        qbe.build_schema_markers_query(),
        ["partitioned_queue"],
        ["partitioned_queue_created_idx"],
        [],
        [],
    )

    assert {(row["kind"], row["name"]) for row in rows} == {
        ("table", "partitioned_queue"),
        ("column", "id"),
        ("column", "created"),
        ("index", "partitioned_queue_created_idx"),
    }


async def test_markers_query_accepts_a_view_backed_table(apgdriver: db.Driver) -> None:
    """The pre-marker information_schema probes accepted views; narrowing broke them."""
    qbe = qb.QueryBuilderEnvironment(DBSettings())
    await apgdriver.execute("CREATE VIEW queue_view AS SELECT 1 AS id;")

    rows = await apgdriver.fetch(qbe.build_schema_markers_query(), ["queue_view"], [], [], [])

    assert ("table", "queue_view") in {(row["kind"], row["name"]) for row in rows}


async def test_type_is_resolved_through_search_path(apgdriver: db.Driver) -> None:
    """search_path installs resolve by visibility, not by current_schema()."""
    q = queries.Queries(apgdriver)
    await apgdriver.execute("CREATE SCHEMA alt;")
    await apgdriver.execute(f"ALTER TYPE {q.qbe.settings.queue_status_type} SET SCHEMA alt;")
    await apgdriver.execute("SET search_path TO public, alt;")

    assert isinstance(await q.schema_check(), schema_revision.SchemaUsable)
