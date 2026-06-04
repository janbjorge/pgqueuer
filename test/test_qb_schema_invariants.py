"""Database-free invariants for the ``pgq install``/``uninstall`` DDL builder."""

from __future__ import annotations

import re

import pytest

from pgqueuer.adapters.persistence.qb import QueryBuilderEnvironment
from pgqueuer.domain.settings import DBSettings, Durability

# Object names sharing no substring with the defaults, so a leaked default
# name in the rendered DDL is unambiguous.
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

CUSTOM_NAMES = [
    CUSTOM.channel,
    CUSTOM.function,
    CUSTOM.statistics_table,
    CUSTOM.queue_status_type,
    CUSTOM.queue_table,
    CUSTOM.queue_table_log,
    CUSTOM.trigger,
    CUSTOM.schedules_table,
]


def _env(settings: DBSettings = CUSTOM) -> QueryBuilderEnvironment:
    return QueryBuilderEnvironment(settings=settings)


def test_every_installed_object_is_uninstalled() -> None:
    """Each TABLE/TYPE/FUNCTION/TRIGGER created by install is dropped by uninstall."""
    install = _env().build_install_query()
    uninstall = _env().build_uninstall_query()

    # The durability token between CREATE and TABLE is "" for logged tables,
    # so the rendered DDL may contain multiple spaces ("CREATE  TABLE").
    created_tables = set(re.findall(r"CREATE\s+(?:UNLOGGED\s+)?TABLE\s+(\w+)", install))
    created_types = set(re.findall(r"CREATE TYPE (\w+)", install))
    created_functions = set(re.findall(r"CREATE FUNCTION (\w+)", install))
    created_triggers = set(re.findall(r"CREATE TRIGGER (\w+)", install))

    dropped_tables = set(re.findall(r"DROP TABLE\s+IF EXISTS\s+(\w+)", uninstall))
    dropped_types = set(re.findall(r"DROP TYPE\s+IF EXISTS\s+(\w+)", uninstall))
    dropped_functions = set(re.findall(r"DROP FUNCTION\s+IF EXISTS\s+(\w+)", uninstall))
    dropped_triggers = set(re.findall(r"DROP TRIGGER\s+IF EXISTS\s+(\w+)", uninstall))

    assert created_tables == {
        CUSTOM.queue_table,
        CUSTOM.queue_table_log,
        CUSTOM.statistics_table,
        CUSTOM.schedules_table,
    }
    assert created_tables <= dropped_tables, created_tables - dropped_tables
    assert created_types <= dropped_types
    assert created_functions <= dropped_functions
    assert created_triggers <= dropped_triggers


def test_uninstall_drops_trigger_before_its_function() -> None:
    """Postgres rejects dropping a function while a trigger still references it."""
    uninstall = _env().build_uninstall_query()
    trig = uninstall.index(f"DROP TRIGGER    IF EXISTS   {CUSTOM.trigger}")
    func = uninstall.index(f"DROP FUNCTION   IF EXISTS   {CUSTOM.function}")
    assert trig < func


def test_install_uses_only_configured_names() -> None:
    install = _env().build_install_query()
    for name in CUSTOM_NAMES:
        assert name in install, f"configured name {name!r} missing from install DDL"
    for default in DEFAULT_NAMES:
        assert default not in install, f"default name {default!r} leaked into install DDL"


def test_notify_function_emits_on_configured_channel() -> None:
    install = _env().build_install_query()
    assert f"pg_notify(\n                '{CUSTOM.channel}'" in install


def test_volatile_durability_marks_all_tables_unlogged() -> None:
    install = QueryBuilderEnvironment(
        settings=DBSettings(durability=Durability.volatile)
    ).build_install_query()
    total_tables = len(re.findall(r"CREATE\s+(?:UNLOGGED\s+)?TABLE\s+\w+", install))
    assert total_tables == 4
    assert install.count("UNLOGGED TABLE") == total_tables


def test_durable_durability_has_no_unlogged_tables() -> None:
    install = QueryBuilderEnvironment(
        settings=DBSettings(durability=Durability.durable)
    ).build_install_query()
    assert "UNLOGGED" not in install


def test_balanced_durability_logs_queue_and_schedules_only() -> None:
    settings = DBSettings(durability=Durability.balanced)
    install = QueryBuilderEnvironment(settings=settings).build_install_query()
    assert install.count("UNLOGGED TABLE") == 2


def test_alter_durability_round_trips_with_install_policy() -> None:
    for level, expected in [
        (Durability.durable, "SET LOGGED"),
        (Durability.volatile, "SET UNLOGGED"),
    ]:
        env = QueryBuilderEnvironment(settings=DBSettings(durability=level))
        stmts = list(env.build_alter_durability_query())
        assert len(stmts) == 4
        assert all(expected in s for s in stmts), (level, stmts)


@pytest.mark.parametrize("level", list(Durability))
def test_install_renders_for_every_durability_level(level: Durability) -> None:
    sql = QueryBuilderEnvironment(settings=DBSettings(durability=level)).build_install_query()
    assert sql.strip().startswith("CREATE TYPE")
