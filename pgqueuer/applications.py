"""Backward-compatibility shim. Canonical: pgqueuer.core.applications"""

from __future__ import annotations

from pgqueuer.core.applications import PgQueuer

__all__ = ["PgQueuer"]
