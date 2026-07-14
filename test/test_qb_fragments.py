"""Text-level invariants between shared SQL fragments and the queries built from them."""

from __future__ import annotations

from pgqueuer.adapters.persistence import qb


def test_notify_function_body_identical_between_install_and_upgrade() -> None:
    env = qb.QueryBuilderEnvironment()
    assert env.notify_function_ddl(replace=True).replace(" OR REPLACE", "", 1) == (
        env.notify_function_ddl(replace=False)
    )
    assert env.notify_function_ddl(replace=False) in env.build_install_query()
    assert env.notify_function_ddl(replace=True) in list(env.build_upgrade_queries())


def test_enqueue_arbiter_matches_dedupe_index_predicate() -> None:
    """ON CONFLICT arbiter inference requires textual match with the partial index."""
    env = qb.QueryBuilderEnvironment()
    queue = qb.QueryQueueBuilder()
    pred = queue.dedupe_key_predicate
    assert pred in queue.build_enqueue_query("skip")
    assert pred in env.dedupe_key_index_ddl(if_not_exists=True)
    assert pred in env.build_install_query()
