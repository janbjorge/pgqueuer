from __future__ import annotations


def test_pgchannel_removed_from_public_api() -> None:
    """PGChannel was removed in v0.27.0 — verify it's gone from re-exports."""
    import pgqueuer.models as m
    import pgqueuer.types as t

    assert not hasattr(m, "PGChannel")
    assert not hasattr(t, "PGChannel")


def test_slot_reexported_from_shims() -> None:
    """Slot reaches both compatibility shims."""
    import pgqueuer.models as m
    import pgqueuer.types as t
    from pgqueuer.domain import types

    assert m.Slot is t.Slot is types.Slot


def test_queue_entrypoint_reexported_from_shims() -> None:
    """QueueEntrypoint reaches both compatibility shims."""
    import pgqueuer.models as m
    import pgqueuer.types as t
    from pgqueuer.domain import types

    assert m.QueueEntrypoint is t.QueueEntrypoint is types.QueueEntrypoint


def test_queue_manager_id_reexported_from_shims() -> None:
    """QueueManagerId reaches both compatibility shims."""
    import pgqueuer.models as m
    import pgqueuer.types as t
    from pgqueuer.domain import types

    assert m.QueueManagerId is t.QueueManagerId is types.QueueManagerId


def test_health_check_id_reexported_from_shims() -> None:
    """HealthCheckId reaches both compatibility shims."""
    import pgqueuer.models as m
    import pgqueuer.types as t
    from pgqueuer.domain import types

    assert m.HealthCheckId is t.HealthCheckId is types.HealthCheckId
