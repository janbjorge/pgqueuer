"""Backward-compatibility shim. Canonical: pgqueuer.ports.driver + pgqueuer.adapters.drivers.*"""
from pgqueuer.adapters.drivers import dsn
from pgqueuer.adapters.drivers.asyncpg import AsyncpgDriver, AsyncpgPoolDriver
from pgqueuer.adapters.drivers.psycopg import (
    PsycopgDriver,
    SyncPsycopgDriver,
    _named_parameter,
    _replace_dollar_named_parameter,
)
from pgqueuer.ports.driver import Driver, SyncDriver

__all__ = [
    "AsyncpgDriver",
    "AsyncpgPoolDriver",
    "Driver",
    "PsycopgDriver",
    "SyncDriver",
    "SyncPsycopgDriver",
    "_named_parameter",
    "_replace_dollar_named_parameter",
    "dsn",
]
