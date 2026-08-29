from __future__ import annotations

import dataclasses

import pytest

from pgqueuer.adapters.persistence.composer import ComposedQuery, SqlComposer


def test_bind_numbers_placeholders_in_order() -> None:
    composer = SqlComposer()
    assert composer.bind(10) == "$1"
    assert composer.bind(["a"]) == "$2"
    assert composer.bind(None) == "$3"
    assert composer.values == [10, ["a"], None]


def test_clauses_puts_each_part_on_its_own_line() -> None:
    assert SqlComposer.clauses("SELECT 1", "FROM jobs") == "SELECT 1\nFROM jobs"


def test_clauses_drops_empty_parts() -> None:
    # An unset filter contributes "" and must leave no blank line behind: the
    # caller never has to own the newline that would otherwise precede it.
    assert SqlComposer.clauses("SELECT 1", "", "FROM jobs", "") == "SELECT 1\nFROM jobs"


def test_clauses_keeps_a_multi_line_part_intact() -> None:
    assert SqlComposer.clauses("SELECT\n    id", "FROM jobs") == "SELECT\n    id\nFROM jobs"


def test_render_pairs_sql_with_bound_args() -> None:
    composer = SqlComposer()
    limit = composer.bind(10)
    composer.cte("ready", f"    SELECT id FROM jobs LIMIT {limit}")
    query = composer.render("SELECT * FROM ready")
    assert query.sql == "WITH\nready AS (\n    SELECT id FROM jobs LIMIT $1\n)\nSELECT * FROM ready"
    assert query.args == (10,)


def test_render_without_fragments_is_the_final_statement() -> None:
    query = SqlComposer().render("SELECT 1")
    assert query.sql == "SELECT 1"
    assert query.args == ()


def test_render_is_repeatable() -> None:
    # render must not consume or mutate composer state.
    composer = SqlComposer()
    composer.bind(1)
    composer.cte("one", "    SELECT 1")
    assert composer.render("SELECT * FROM one") == composer.render("SELECT * FROM one")


def test_ctes_are_chained_in_insertion_order() -> None:
    composer = SqlComposer()
    composer.cte("first", "    SELECT 1")
    composer.cte("second", "    SELECT 2")
    query = composer.render("SELECT * FROM second")
    assert query.sql == (
        "WITH\nfirst AS (\n    SELECT 1\n),\n\nsecond AS (\n    SELECT 2\n)\nSELECT * FROM second"
    )


def test_cte_indents_a_body_written_at_python_indentation() -> None:
    # The caller writes SQL wherever the surrounding Python sits; the composer
    # strips that indentation and applies the one level a CTE body needs.
    composer = SqlComposer()
    composer.cte(
        "ready",
        """
            SELECT id
            FROM jobs
        """,
    )
    query = composer.render("SELECT * FROM ready")
    assert query.sql == "WITH\nready AS (\n    SELECT id\n    FROM jobs\n)\nSELECT * FROM ready"


def test_cte_keeps_indentation_relative_within_a_clause() -> None:
    composer = SqlComposer()
    composer.cte(
        "ready",
        """
            SELECT id FROM (
                SELECT id FROM jobs
            ) inner_jobs
        """,
    )
    body = "    SELECT id FROM (\n        SELECT id FROM jobs\n    ) inner_jobs"
    assert composer.render("SELECT * FROM ready").sql == (
        f"WITH\nready AS (\n{body}\n)\nSELECT * FROM ready"
    )


def test_cte_joins_clauses_and_drops_empty_ones() -> None:
    composer = SqlComposer()
    composer.cte("ready", "SELECT id", "", "FROM jobs")
    query = composer.render("SELECT * FROM ready")
    assert query.sql == "WITH\nready AS (\n    SELECT id\n    FROM jobs\n)\nSELECT * FROM ready"


def test_cte_without_comment_has_no_header() -> None:
    composer = SqlComposer()
    composer.cte("ready", "SELECT 1")
    assert composer.render("SELECT * FROM ready").sql.startswith("WITH\nready AS (")


def test_cte_comment_renders_as_a_line_comment() -> None:
    composer = SqlComposer()
    composer.cte("ready", "SELECT 1", comment="Jobs ready to run.")
    query = composer.render("SELECT * FROM ready")
    assert query.sql == (
        "WITH\n-- Jobs ready to run.\nready AS (\n    SELECT 1\n)\nSELECT * FROM ready"
    )


def test_cte_rejects_a_multi_line_comment() -> None:
    # A comment orients whoever reads the statement in a Postgres log. Rationale
    # that needs a paragraph belongs in a Python comment, not in the rendered SQL.
    composer = SqlComposer()
    with pytest.raises(ValueError, match="single line"):
        composer.cte("ready", "SELECT 1", comment="First line.\nSecond line.")


def test_where_chains_conditions_under_one_keyword() -> None:
    assert SqlComposer.where("a = 1", "b = 2") == "WHERE a = 1\n  AND b = 2"


def test_where_drops_empty_conditions() -> None:
    # An inactive gate contributes "" and must not leave a dangling AND behind.
    assert SqlComposer.where("a = 1", "", "b = 2") == "WHERE a = 1\n  AND b = 2"


def test_where_without_conditions_renders_nothing() -> None:
    assert SqlComposer.where("") == ""
    assert SqlComposer.where() == ""


def test_where_aligns_every_condition_under_the_keyword() -> None:
    # AND and OR are different widths; both must leave the conditions in one
    # column so the caller never counts spaces to line them up.
    conjunction = SqlComposer.where("a = 1", "b = 2")
    disjunction = SqlComposer.where("a = 1", "b = 2", joiner="OR")
    assert conjunction == "WHERE a = 1\n  AND b = 2"
    assert disjunction == "WHERE a = 1\n   OR b = 2"
    starts = {line.index("a = 1" if "a" in line else "b = 2") for line in conjunction.splitlines()}
    assert starts == {len("WHERE ")}


def test_nest_indents_the_body_under_the_header() -> None:
    assert SqlComposer.nest("SELECT", "id,", "priority") == "SELECT\n    id,\n    priority"


def test_nest_closes_with_a_footer() -> None:
    nested = SqlComposer.nest("EXISTS (", "SELECT 1 FROM jobs", footer=") AS any_job")
    assert nested == "EXISTS (\n    SELECT 1 FROM jobs\n) AS any_job"


def test_nest_drops_empty_body_parts() -> None:
    assert SqlComposer.nest("SELECT", "id", "") == "SELECT\n    id"


def test_nest_deepens_indentation_when_composed() -> None:
    # Nesting depth comes from the call structure, so an inner block indents
    # relative to its parent without the caller typing either level.
    inner = SqlComposer.nest("CASE", "WHEN a THEN 1", footer="END")
    outer = SqlComposer.nest("SELECT", inner)
    assert outer == "SELECT\n    CASE\n        WHEN a THEN 1\n    END"


def test_composed_query_is_immutable() -> None:
    query = ComposedQuery(sql="SELECT 1", args=())
    with pytest.raises(dataclasses.FrozenInstanceError):
        query.sql = "SELECT 2"  # type: ignore[misc]
