"""Backward-compatibility shim. Canonical: pgqueuer.ports.driver + pgqueuer.adapters.drivers.*"""

from __future__ import annotations

from pgqueuer.adapters.drivers.asyncpg import AsyncpgDriver
from pgqueuer.adapters.drivers.psycopg import PsycopgDriver, SyncPsycopgDriver
from pgqueuer.ports.driver import Driver, SyncDriver

__all__ = [
    "AsyncpgDriver",
    "Driver",
    "PsycopgDriver",
    "SyncDriver",
    "SyncPsycopgDriver",
]
