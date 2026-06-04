"""Unit tests for ``QueueManager.entrypoint`` input validation."""

from __future__ import annotations

import pytest

from pgqueuer.adapters.inmemory import InMemoryDriver, InMemoryQueries
from pgqueuer.core.qm import QueueManager
from pgqueuer.domain.models import Job


def _qm() -> QueueManager:
    return QueueManager(InMemoryQueries(driver=InMemoryDriver()))


async def _handler(job: Job) -> None:
    return None


def test_duplicate_name_is_rejected() -> None:
    qm = _qm()
    qm.entrypoint("dup")(_handler)

    with pytest.raises(RuntimeError, match="already in registry"):
        qm.entrypoint("dup")(_handler)


def test_duplicate_name_is_rejected_at_decorator_creation() -> None:
    """The error is raised by entrypoint(...) before the inner decorator runs."""
    qm = _qm()
    qm.entrypoint("once")(_handler)
    with pytest.raises(RuntimeError, match="already in registry"):
        qm.entrypoint("once")


def test_non_integer_concurrency_limit_is_rejected() -> None:
    qm = _qm()
    with pytest.raises(ValueError, match="Concurrency limit must be int"):
        qm.entrypoint("ep", concurrency_limit="5")  # type: ignore[arg-type]


def test_boolean_concurrency_limit_is_accepted_today() -> None:
    """bool is an int subclass, so True (== 1) is accepted; pin the behaviour."""
    qm = _qm()
    qm.entrypoint("flag", concurrency_limit=True)(_handler)
    assert "flag" in qm.entrypoint_registry


def test_negative_concurrency_limit_is_rejected() -> None:
    qm = _qm()
    with pytest.raises(ValueError, match="greater or eq"):
        qm.entrypoint("ep", concurrency_limit=-1)


def test_zero_concurrency_limit_is_accepted() -> None:
    qm = _qm()
    qm.entrypoint("unlimited", concurrency_limit=0)(_handler)
    assert "unlimited" in qm.entrypoint_registry


def test_non_boolean_accepts_context_is_rejected() -> None:
    qm = _qm()
    with pytest.raises(ValueError, match="accepts_context must be boolean or None"):
        qm.entrypoint("ep", accepts_context="yes")(_handler)  # type: ignore[arg-type]


@pytest.mark.parametrize("value", [True, False, None])
def test_valid_accepts_context_values(value: bool | None) -> None:
    qm = _qm()
    qm.entrypoint("ep", accepts_context=value)(_handler)
    assert "ep" in qm.entrypoint_registry


def test_unknown_on_failure_is_rejected() -> None:
    qm = _qm()
    with pytest.raises(ValueError, match="on_failure must be one of"):
        qm.entrypoint("ep", on_failure="explode")(_handler)  # type: ignore[arg-type]


@pytest.mark.parametrize("policy", ["delete", "hold"])
def test_valid_on_failure_values(policy: str) -> None:
    qm = _qm()
    qm.entrypoint("ep", on_failure=policy)(_handler)  # type: ignore[arg-type]
    assert "ep" in qm.entrypoint_registry
