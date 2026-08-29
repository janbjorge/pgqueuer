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


def test_block_dedents_to_the_left_margin() -> None:
    assert SqlComposer.block("\n    SELECT 1\n    FROM jobs\n") == "SELECT 1\nFROM jobs"


def test_block_keeps_indentation_relative_within_the_block() -> None:
    nested = """
        SELECT id FROM (
            SELECT id FROM jobs
        ) inner_jobs
    """
    assert SqlComposer.block(nested) == "SELECT id FROM (\n    SELECT id FROM jobs\n) inner_jobs"


def test_block_drops_the_line_an_elided_clause_leaves_behind() -> None:
    # A clause only some query shapes carry is interpolated on its own line.
    # When it renders empty the line must go with it, rather than leaving a
    # blank line in the middle of the statement.
    for gate in ("WHERE priority > 0", ""):
        rendered = SqlComposer.block(f"""
            SELECT id
            FROM jobs
            {gate}
            ORDER BY id
        """)
        assert "\n\n" not in rendered
        assert rendered.splitlines()[-1] == "ORDER BY id"
    assert SqlComposer.block("\n    SELECT id\n    \n    FROM jobs\n") == "SELECT id\nFROM jobs"


def test_block_measures_the_margin_without_the_elided_line() -> None:
    # The whitespace left where a clause was elided is shorter than the block's
    # own indentation; dedent must ignore it instead of measuring the margin
    # against it and under-indenting every other line.
    assert SqlComposer.block("\n        SELECT 1\n  \n        FROM jobs\n") == "SELECT 1\nFROM jobs"


def test_render_pairs_sql_with_bound_args() -> None:
    composer = SqlComposer()
    limit = composer.bind(10)
    composer.cte("ready", f"SELECT id FROM jobs LIMIT {limit}")
    query = composer.render("SELECT * FROM ready")
    assert query.sql == "WITH\nready AS (\n    SELECT id FROM jobs LIMIT $1\n)\nSELECT * FROM ready"
    assert query.args == (10,)


def test_render_normalises_the_final_statement() -> None:
    # The final statement carries optional clauses just like a CTE body does.
    query = SqlComposer().render("""
        SELECT 1

        FROM jobs
    """)
    assert query.sql == "SELECT 1\nFROM jobs"


def test_render_without_fragments_is_the_final_statement() -> None:
    query = SqlComposer().render("SELECT 1")
    assert query.sql == "SELECT 1"
    assert query.args == ()


def test_render_is_repeatable() -> None:
    # render must not consume or mutate composer state.
    composer = SqlComposer()
    composer.bind(1)
    composer.cte("one", "SELECT 1")
    assert composer.render("SELECT * FROM one") == composer.render("SELECT * FROM one")


def test_ctes_are_chained_in_insertion_order() -> None:
    composer = SqlComposer()
    composer.cte("first", "SELECT 1")
    composer.cte("second", "SELECT 2")
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


def test_cte_drops_a_line_left_empty_by_an_inapplicable_clause() -> None:
    composer = SqlComposer()
    gate = ""
    composer.cte(
        "ready",
        f"""
            SELECT id
            FROM jobs
            {gate}
        """,
    )
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


def test_composed_query_is_immutable() -> None:
    query = ComposedQuery(sql="SELECT 1", args=())
    with pytest.raises(dataclasses.FrozenInstanceError):
        query.sql = "SELECT 2"  # type: ignore[misc]
