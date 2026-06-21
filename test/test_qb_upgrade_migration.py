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
    """pgq upgrade may run repeatedly, so every statement must be a no-op when applied."""
    idempotent_markers = ("IF NOT EXISTS", "OR REPLACE", "IF EXISTS", "ADD VALUE IF NOT EXISTS")
    for stmt in QueryBuilderEnvironment(settings=CUSTOM).build_upgrade_queries():
        head = stmt.strip().splitlines()[0]
        # ALTER COLUMN ... TYPE is idempotent on its own and has no IF-EXISTS form.
        if "ALTER COLUMN status TYPE" in stmt:
            continue
        assert any(m in stmt for m in idempotent_markers), f"non-idempotent upgrade stmt: {head}"


INSTALL_SQL = QueryBuilderEnvironment(settings=CUSTOM).build_install_query()


def test_install_creates_entrypoint_leading_dequeue_indexes() -> None:
    """Install creates the partial indexes the dequeue LATERAL relies on (#668)."""
    assert f"{CUSTOM.queue_table}_ep_prio_id_idx" in INSTALL_SQL
    assert f"{CUSTOM.queue_table}_ep_ea_idx" in INSTALL_SQL
    assert "(entrypoint, priority DESC, id ASC)" in INSTALL_SQL
    assert "(entrypoint, execute_after)" in INSTALL_SQL
    assert INSTALL_SQL.count("WHERE status = 'queued'") >= 2


def test_install_aggregation_index_is_functional_not_placeholder() -> None:
    """Log aggregation index is the real composite, not the ((1)) placeholder (#668)."""
    assert "((1))" not in INSTALL_SQL
    assert "(entrypoint, priority, status, created) WHERE not aggregated" in INSTALL_SQL


def test_upgrade_creates_performance_indexes() -> None:
    """pgq upgrade adds the same dequeue and aggregation indexes (#668)."""
    upgrade_sql = _upgrade_sql()
    assert f"{CUSTOM.queue_table}_ep_prio_id_idx" in upgrade_sql
    assert f"{CUSTOM.queue_table}_ep_ea_idx" in upgrade_sql
    assert "((1))" not in upgrade_sql
    assert "(entrypoint, priority, status, created) WHERE not aggregated" in upgrade_sql
