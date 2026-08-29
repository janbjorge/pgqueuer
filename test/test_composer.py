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


def test_cte_bodies_are_embedded_verbatim() -> None:
    # The composer must never rewrite a body: a blank line or a unicode line
    # separator inside a SQL string literal is part of the literal's value.
    composer = SqlComposer()
    composer.cte("greeting", "    SELECT 'a\n\nb' AS blank, 'c\u2028d' AS separator")
    query = composer.render("SELECT * FROM greeting")
    assert "'a\n\nb'" in query.sql
    assert "'c\u2028d'" in query.sql


def test_cte_without_comment_has_no_header() -> None:
    composer = SqlComposer()
    composer.cte("ready", "    SELECT 1")
    assert composer.render("SELECT * FROM ready").sql.startswith("WITH\nready AS (")


def test_cte_comment_renders_as_line_comments() -> None:
    composer = SqlComposer()
    composer.cte(
        "ready",
        "    SELECT 1",
        comment="""
            First line.
            Second line.
        """,
    )
    query = composer.render("SELECT * FROM ready")
    assert query.sql == (
        "WITH\n-- First line.\n-- Second line.\nready AS (\n    SELECT 1\n)\nSELECT * FROM ready"
    )


def test_composed_query_is_immutable() -> None:
    query = ComposedQuery(sql="SELECT 1", args=())
    with pytest.raises(dataclasses.FrozenInstanceError):
        query.sql = "SELECT 2"  # type: ignore[misc]
