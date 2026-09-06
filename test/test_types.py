from __future__ import annotations


def test_pgchannel_removed_from_public_api() -> None:
    """PGChannel was removed in v0.27.0 — verify it's gone from re-exports."""
    import pgqueuer.models as m
    import pgqueuer.types as t

    assert not hasattr(m, "PGChannel")
    assert not hasattr(t, "PGChannel")


def test_identity_newtypes_reexported_from_shims() -> None:
    """QueueEntrypoint, QueueManagerId and Slot reach both compatibility shims."""
    import pgqueuer.models as m
    import pgqueuer.types as t
    from pgqueuer.domain import types

    assert m.QueueEntrypoint is t.QueueEntrypoint is types.QueueEntrypoint
    assert m.QueueManagerId is t.QueueManagerId is types.QueueManagerId
    assert m.Slot is t.Slot is types.Slot
