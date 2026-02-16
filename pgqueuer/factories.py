"""Backward-compatibility shim. Canonical: pgqueuer.adapters.cli.factories"""
from pgqueuer.adapters.cli.factories import (
    load_factory,
    run_factory,
)

__all__ = [
    "load_factory",
    "run_factory",
]
