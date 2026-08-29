"""The log_statistics statement carries only the clauses its filters ask for."""

from __future__ import annotations

import re
from datetime import timedelta

import pytest

from pgqueuer.adapters.persistence import qb
from pgqueuer.adapters.persistence.composer import ComposedQuery
from pgqueuer.domain.settings import DBSettings


def compose(limit: int | None, last: timedelta | None) -> ComposedQuery:
    # Pin the table name so a developer's exported PGQUEUER_* env vars cannot
    # skew the rendered SQL.
    settings = DBSettings(db_schema=None, statistics_table="pgqueuer_statistics")
    builder = qb.QueryQueueBuilder(settings=settings)
    return builder.build_log_statistics_query(limit=limit, last=last)


@pytest.mark.parametrize(
    ("limit", "last", "has_where", "has_limit"),
    [
        (None, None, False, False),
        (10, None, False, True),
        (None, timedelta(hours=1), True, False),
        (10, timedelta(hours=1), True, True),
    ],
)
def test_log_statistics_shape_follows_filters(
    limit: int | None,
    last: timedelta | None,
    has_where: bool,
    has_limit: bool,
) -> None:
    query = compose(limit, last)
    assert ("WHERE" in query.sql) == has_where
    assert ("LIMIT" in query.sql) == has_limit
    # Every $N placeholder maps onto exactly one bound arg: no gaps, no extras.
    placeholders = {int(n) for n in re.findall(r"\$(\d+)", query.sql)}
    assert placeholders == set(range(1, len(query.args) + 1))


def test_log_statistics_full_shape() -> None:
    expected = """SELECT
        count,
        created,
        entrypoint,
        priority,
        status
    FROM pgqueuer_statistics
    WHERE created > NOW() - $1::interval
    ORDER BY id DESC
    LIMIT $2
    """
    query = compose(10, timedelta(hours=1))
    assert query.sql == expected
    assert query.args == (timedelta(hours=1), 10)
