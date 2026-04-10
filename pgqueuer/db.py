"""Backward-compatibility shim. Canonical: pgqueuer.ports.driver + pgqueuer.adapters.drivers.*"""

from pgqueuer.adapters.drivers.asyncpg import AsyncpgDriver, AsyncpgPoolDriver
from pgqueuer.adapters.drivers.psycopg import PsycopgDriver, SyncPsycopgDriver
from pgqueuer.ports.driver import Driver, SyncDriver

__all__ = [
    "AsyncpgDriver",
    "AsyncpgPoolDriver",
    "Driver",
    "PsycopgDriver",
    "SyncDriver",
    "SyncPsycopgDriver",
]
