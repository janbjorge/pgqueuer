import pytest

from pgqueuer.qb import Durability, DurabilityPolicy


@pytest.mark.parametrize(
    "durability,expected_policy",
    [
        (
            Durability.volatile,
            DurabilityPolicy(
                queue_table="UNLOGGED",
                queue_log_table="UNLOGGED",
                statistics_table="UNLOGGED",
                schedules_table="UNLOGGED",
            ),
        ),
        (
            Durability.balanced,
            DurabilityPolicy(
                queue_table="",
                queue_log_table="UNLOGGED",
                statistics_table="UNLOGGED",
                schedules_table="",
            ),
        ),
        (
            Durability.durable,
            DurabilityPolicy(
                queue_table="",
                queue_log_table="",
                statistics_table="",
                schedules_table="",
            ),
        ),
    ],
)
def test_durability_policy(durability: Durability, expected_policy: DurabilityPolicy) -> None:
    """
    Test that the `config` property of each `Durability` level correctly returns
    the expected `DurabilityPolicy`.
    """
    assert durability.config == expected_policy


@pytest.mark.parametrize(
    "policy,expected_logged",
    [
        (
            DurabilityPolicy(
                queue_table="",
                queue_log_table="UNLOGGED",
                statistics_table="UNLOGGED",
                schedules_table="",
            ),
            {
                "pgqueuer": True,
                "pgqueuer_log": False,
                "pgqueuer_statistics": False,
                "pgqueuer_schedules": True,
            },
        ),
        (
            DurabilityPolicy(
                queue_table="UNLOGGED",
                queue_log_table="UNLOGGED",
                statistics_table="UNLOGGED",
                schedules_table="UNLOGGED",
            ),
            {
                "pgqueuer": False,
                "pgqueuer_log": False,
                "pgqueuer_statistics": False,
                "pgqueuer_schedules": False,
            },
        ),
    ],
)
def test_policy_attributes(policy: DurabilityPolicy, expected_logged: dict[str, bool]) -> None:
    """
    Test that individual attributes in `DurabilityPolicy` correctly indicate whether they
    are logged or unlogged.
    """
    assert (policy.queue_table == "") == expected_logged["pgqueuer"]
    assert (policy.queue_log_table == "") == expected_logged["pgqueuer_log"]
    assert (policy.statistics_table == "") == expected_logged["pgqueuer_statistics"]
    assert (policy.schedules_table == "") == expected_logged["pgqueuer_schedules"]


def test_invalid_durability_level() -> None:
    """
    Test that accessing an unknown durability level raises a ValueError.
    """
    with pytest.raises(ValueError, match="'nonexistent' is not a valid Durability"):
        Durability("nonexistent")
