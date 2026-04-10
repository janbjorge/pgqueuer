from __future__ import annotations

from collections.abc import Generator

import pytest

from pgqueuer.adapters.cli.cli import AppConfig
from pgqueuer.adapters.drivers import dsn
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


def test_dsn_with_schema() -> None:
    result = dsn(user="u", password="p", host="h", port="5432", database="db", schema="myschema")
    assert result == "postgresql://u:p@h:5432/db?options=-csearch_path%3Dmyschema"


def test_dsn_without_schema() -> None:
    result = dsn(user="u", password="p", host="h", port="5432", database="db")
    assert result == "postgresql://u:p@h:5432/db"


def test_app_config_dsn_appends_schema_to_user_provided_dsn() -> None:
    config = AppConfig(pg_dsn="postgresql://u:p@h:5432/db", pg_schema="myschema")
    assert config.dsn == "postgresql://u:p@h:5432/db?options=-csearch_path%3Dmyschema"


def test_app_config_dsn_appends_schema_to_dsn_with_query() -> None:
    config = AppConfig(pg_dsn="postgresql://u:p@h:5432/db?sslmode=require", pg_schema="myschema")
    assert (
        config.dsn == "postgresql://u:p@h:5432/db?sslmode=require&options=-csearch_path%3Dmyschema"
    )


def test_app_config_dsn_no_schema() -> None:
    config = AppConfig(pg_dsn="postgresql://u:p@h:5432/db")
    assert config.dsn == "postgresql://u:p@h:5432/db"
