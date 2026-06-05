"""Backward-compatibility shim. Canonical: pgqueuer.adapters.cli.factories"""

from __future__ import annotations

from pgqueuer.adapters.cli.factories import (
    load_factory,
    validate_factory_result,
)

__all__ = [
    "load_factory",
    "validate_factory_result",
]
