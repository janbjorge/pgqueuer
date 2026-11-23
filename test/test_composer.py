from __future__ import annotations

import dataclasses
from typing import NamedTuple

import pytest

from pgqueuer.adapters.persistence.composer import (
    ComposedQuery,
    Cte,
    SqlComposer,
    TemplateComposer,
    block,
    render_cte,
    render_statement,
)


class Request(NamedTuple):
    limit: int
    tags: list[str]


def test_bind_numbers_placeholders_in_order() -> None:
    composer = SqlComposer()
    assert composer.bind(10) == "$1"
    assert composer.bind(["a"]) == "$2"
    assert composer.bind(None) == "$3"
    assert composer.values == [10, ["a"], None]


def test_block_dedents_to_the_left_margin() -> None:
    assert block("\n    SELECT 1\n    FROM jobs\n") == "SELECT 1\nFROM jobs"


def test_block_keeps_indentation_relative_within_the_block() -> None:
    nested = """
        SELECT id FROM (
            SELECT id FROM jobs
        ) inner_jobs
    """
    assert block(nested) == "SELECT id FROM (\n    SELECT id FROM jobs\n) inner_jobs"


def test_block_drops_the_line_an_elided_clause_leaves_behind() -> None:
    # A clause only some query shapes carry is interpolated on its own line.
    # When it renders empty the line must go with it, rather than leaving a
    # blank line in the middle of the statement.
    for gate in ("WHERE priority > 0", ""):
        rendered = block(f"""
            SELECT id
            FROM jobs
            {gate}
            ORDER BY id
        """)
        assert "\n\n" not in rendered
        assert rendered.splitlines()[-1] == "ORDER BY id"
    assert block("\n    SELECT id\n    \n    FROM jobs\n") == "SELECT id\nFROM jobs"


def test_block_measures_the_margin_without_the_elided_line() -> None:
    # The whitespace left where a clause was elided is shorter than the block's
    # own indentation; dedent must ignore it instead of measuring the margin
    # against it and under-indenting every other line.
    assert block("\n        SELECT 1\n  \n        FROM jobs\n") == "SELECT 1\nFROM jobs"


def test_render_pairs_sql_with_bound_args() -> None:
    composer = SqlComposer()
    limit = composer.bind(10)
    query = composer.render(
        [Cte("ready", f"SELECT id FROM jobs LIMIT {limit}")], "SELECT * FROM ready"
    )
    assert query.sql == "WITH\nready AS (\n    SELECT id FROM jobs LIMIT $1\n)\nSELECT * FROM ready"
    assert query.args == (10,)


def test_render_normalises_the_final_statement() -> None:
    # The final statement carries optional clauses just like a CTE body does.
    assert (
        render_statement(
            [],
            """
        SELECT 1

        FROM jobs
    """,
        )
        == "SELECT 1\nFROM jobs"
    )


def test_render_without_fragments_is_the_final_statement() -> None:
    query = SqlComposer().render([], "SELECT 1")
    assert query.sql == "SELECT 1"
    assert query.args == ()


def test_render_is_repeatable() -> None:
    # render must not consume or mutate composer state.
    composer = SqlComposer()
    composer.bind(1)
    ctes = [Cte("one", "SELECT 1")]
    assert composer.render(ctes, "SELECT * FROM one") == composer.render(ctes, "SELECT * FROM one")


def test_ctes_are_chained_in_order() -> None:
    sql = render_statement([Cte("first", "SELECT 1"), Cte("second", "SELECT 2")], "SELECT 1")
    assert sql == "WITH\nfirst AS (\n    SELECT 1\n),\n\nsecond AS (\n    SELECT 2\n)\nSELECT 1"


def test_cte_indents_a_body_written_at_python_indentation() -> None:
    # The caller writes SQL wherever the surrounding Python sits; the renderer
    # strips that indentation and applies the one level a CTE body needs.
    ready = Cte(
        "ready",
        """
            SELECT id
            FROM jobs
        """,
    )
    assert render_cte(ready) == "ready AS (\n    SELECT id\n    FROM jobs\n)"


def test_cte_drops_a_line_left_empty_by_an_inapplicable_clause() -> None:
    gate = ""
    ready = Cte(
        "ready",
        f"""
            SELECT id
            FROM jobs
            {gate}
        """,
    )
    assert render_cte(ready) == "ready AS (\n    SELECT id\n    FROM jobs\n)"


def test_cte_without_comment_has_no_header() -> None:
    assert render_cte(Cte("ready", "SELECT 1")).startswith("ready AS (")


def test_cte_comment_renders_as_a_line_comment() -> None:
    ready = Cte("ready", "SELECT 1", comment="Jobs ready to run.")
    assert render_cte(ready) == "-- Jobs ready to run.\nready AS (\n    SELECT 1\n)"


def test_cte_rejects_a_multi_line_comment() -> None:
    # A comment orients whoever reads the statement in a Postgres log. Rationale
    # that needs a paragraph belongs in a Python comment, not in the rendered SQL.
    with pytest.raises(ValueError, match="single line"):
        Cte("ready", "SELECT 1", comment="First line.\nSecond line.")


def test_render_rejects_a_cte_defined_twice() -> None:
    # Shape variants share a CTE name; rendering both would be invalid SQL.
    with pytest.raises(ValueError, match="defined twice"):
        render_statement([Cte("available", "SELECT 1"), Cte("available", "SELECT 2")], "SELECT 1")


def test_template_reads_each_param_from_the_call_args() -> None:
    composer = TemplateComposer[Request]()
    tags = composer.param(lambda request: request.tags)
    limit = composer.param(lambda request: request.limit)
    template = composer.render(
        [Cte("ready", f"SELECT id FROM jobs WHERE tag = ANY({tags})")],
        f"SELECT * FROM ready LIMIT {limit}",
    )
    assert template.sql.endswith("SELECT * FROM ready LIMIT $2")
    assert template.bind(Request(limit=5, tags=["a"])).args == (["a"], 5)
    assert template.bind(Request(limit=7, tags=["b"])).args == (["b"], 7)


def test_composed_query_is_immutable() -> None:
    query = ComposedQuery(sql="SELECT 1", args=())
    with pytest.raises(dataclasses.FrozenInstanceError):
        query.sql = "SELECT 2"  # type: ignore[misc]
