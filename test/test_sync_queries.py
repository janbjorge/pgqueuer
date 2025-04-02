import pytest

from pgqueuer import db, queries


@pytest.mark.parametrize("N", (1, 2, 64))
def test_queries_put(pgdriver: db.SyncDriver, N: int) -> None:
    q = queries.SyncQueries(pgdriver)

    assert sum(x.count for x in q.queue_size()) == 0

    for _ in range(N):
        q.enqueue("placeholder", None)

    assert sum(x.count for x in q.queue_size()) == N
