"""
Smoke test for Issue #536: Race Condition in Scheduler

Verifies that the critical SQL fix is in place.
"""

from pgqueuer.adapters.persistence import qb


def test_issue_536_heartbeat_in_query() -> None:
    """
    Smoke test: Verify heartbeat = NOW() is in the SQL update query.

    Before Fix: ❌ FAILS (heartbeat = NOW() missing)
    After Fix:  ✅ PASSES (heartbeat = NOW() present)

    This prevents the race condition where multiple scheduler instances
    execute the same scheduled task multiple times instead of once.
    """
    builder = qb.QuerySchedulerBuilder()
    query = builder.build_fetch_schedule_query()

    assert "heartbeat = NOW()" in query, (
        "CRITICAL: Missing 'heartbeat = NOW()' in build_fetch_schedule_query(). "
        "This causes Issue #536 race condition."
    )
