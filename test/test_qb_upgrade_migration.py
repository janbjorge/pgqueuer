"""Database-free invariants for the ``pgq upgrade`` migration builder."""

from __future__ import annotations

from pgqueuer.adapters.persistence.qb import QueryBuilderEnvironment
from pgqueuer.domain.settings import DBSettings

# Object names sharing no substring with the defaults, so a hardcoded
# default name in the migration is unambiguous.
CUSTOM = DBSettings(
    channel="acme_ch",
    function="acme_fn_changed",
    statistics_table="acme_stats",
    queue_status_type="acme_status",
    queue_table="acme_jobs",
    queue_table_log="acme_jobs_log",
    trigger="acme_tg_changed",
    schedules_table="acme_schedules",
)

DEFAULT_NAMES = [
    DBSettings().channel,
    DBSettings().function,
    DBSettings().statistics_table,
    DBSettings().queue_status_type,
    DBSettings().queue_table,
    DBSettings().queue_table_log,
    DBSettings().trigger,
    DBSettings().schedules_table,
]


def _upgrade_sql() -> str:
    return "\n".join(QueryBuilderEnvironment(settings=CUSTOM).build_upgrade_queries())


def test_upgrade_uses_only_configured_names() -> None:
    """The statistics-status cast must target the configured type, not a hardcoded one."""
    upgrade_sql = _upgrade_sql()
    for default in DEFAULT_NAMES:
        assert default not in upgrade_sql, f"default name {default!r} leaked into upgrade DDL"
    assert "::pgqueuer_status" not in upgrade_sql
    assert f"USING status::TEXT::{CUSTOM.queue_status_type}" in upgrade_sql


def test_every_upgrade_statement_is_idempotent() -> None:
    """pgq upgrade may run repeatedly, so every statement must be a no-op when applied.

    Two statement shapes are idempotent without a marker: DO-blocks, which must
    guard their DDL with catalog checks, and ALTER COLUMN ... TYPE, where a
    re-run re-casts to the same type. Their no-op behavior is proven by the
    DB-backed tests that run upgrade() twice (test_schema_id_widen).
    """
    idempotent_markers = ("IF NOT EXISTS", "OR REPLACE", "IF EXISTS", "ADD VALUE IF NOT EXISTS")
    for stmt in QueryBuilderEnvironment(settings=CUSTOM).build_upgrade_queries():
        head = stmt.strip().splitlines()[0]
        if stmt.strip().startswith("DO $$"):
            continue
        if "ALTER COLUMN" in stmt and " TYPE " in stmt:
            continue
        assert any(m in stmt for m in idempotent_markers), f"non-idempotent upgrade stmt: {head}"
