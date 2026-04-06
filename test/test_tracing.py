from __future__ import annotations


def test_tracing_singleton_in_ports() -> None:
    """TracingConfig and TRACER live in pgqueuer.ports.tracing."""
    from pgqueuer.ports.tracing import TRACER, TracingConfig, set_tracing_class

    assert isinstance(TRACER, TracingConfig)
    assert callable(set_tracing_class)
