from __future__ import annotations

from collections.abc import Generator

import pytest

from pgqueuer.adapters.cli.cli import _add_schema_to_dsn
from pgqueuer.adapters.persistence.query_helpers import merge_tracing_headers


@pytest.mark.parametrize(
    "headers, trace_headers, expected",
    [
        (
            [{"key1": "value1"}],
            [{"trace_key": "trace_value"}],
            [{"key1": "value1", "trace_key": "trace_value"}],
        ),
        (
            [None],
            [{"trace_key": "trace_value"}],
            [{"trace_key": "trace_value"}],
        ),
        (
            [{"key1": "value1"}],
            [None],
            [{"key1": "value1"}],
        ),
        (
            [None],
            [None],
            [{}],
        ),
    ],
)
def test_merge_tracing_headers(
    headers: list[dict | None],
    trace_headers: Generator[dict | None, None, None],
    expected: list[dict],
) -> None:
    assert merge_tracing_headers(headers, trace_headers) == expected


def test_add_schema_to_empty_dsn() -> None:
    dsn = "postgresql://user:password@host:port/dbname"
    schema = "myschema"
    expected = "postgresql://user:password@host:port/dbname?options=-csearch_path%3Dmyschema"
    assert _add_schema_to_dsn(dsn, schema) == expected


def test_add_schema_to_dsn_with_existing_query() -> None:
    dsn = "postgresql://user:password@host:port/dbname?sslmode=require"
    schema = "myschema"
    expected = "postgresql://user:password@host:port/dbname?sslmode=require&options=-csearch_path%3Dmyschema"
    assert _add_schema_to_dsn(dsn, schema) == expected


def test_raise_on_existing_search_path() -> None:
    dsn = "postgresql://user:password@host:port/dbname?options=-c+search_path=otherschema"
    schema = "myschema"
    with pytest.raises(ValueError, match="search_path is already set in the options parameter."):
        _add_schema_to_dsn(dsn, schema)


def test_preserve_other_options_and_add_search_path() -> None:
    dsn = "postgresql://user:password@host:port/dbname?options=-cother_option=foo"
    schema = "myschema"
    expected = "postgresql://user:password@host:port/dbname?options=-cother_option%3Dfoo&options=-csearch_path%3Dmyschema"
    assert _add_schema_to_dsn(dsn, schema) == expected
