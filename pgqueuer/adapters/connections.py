"""Shared connection/pool factories consuming ``ConnectionSettings``.

Design rules (issue #701, continuing #605):

- The DSN is never parsed, inspected, or rebuilt here. It is passed to the
  driver verbatim, so multi-host DSNs, ``sslmode``, ``options``,
  ``target_session_attrs``, ``service=`` and any future libpq parameter
  survive untouched.
- A connect kwarg is only passed when the user explicitly configured it
  (``PGQUEUER_*`` env var or API argument). asyncpg gives explicit kwargs
  precedence over DSN parameters, so an unconditional default would silently
  clobber user settings.
- The psycopg path never forwards libpq-native env vars such as
  ``PGCONNECT_TIMEOUT`` or ``PGAPPNAME``, since libpq already reads those
  with correct precedence. Only ``PGQUEUER_*`` values are forwarded.
"""

from __future__ import annotations

import math
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, TypedDict

from pgqueuer.domain.settings import ConnectionSettings

if TYPE_CHECKING:
    import asyncpg
    import psycopg
    import psycopg_pool


class AsyncpgConnectKwargs(TypedDict, total=False):
    timeout: float
    server_settings: dict[str, str]


class PsycopgConnectKwargs(TypedDict, total=False):
    autocommit: bool
    connect_timeout: int
    application_name: str


def _resolve(
    dsn: str | None,
    settings: ConnectionSettings | None,
) -> tuple[str | None, ConnectionSettings]:
    """Resolution ladder: explicit ``dsn`` argument > ``settings.dsn`` (env-backed)."""
    settings = settings or ConnectionSettings()
    return (dsn if dsn is not None else settings.dsn), settings


def _asyncpg_connect_kwargs(settings: ConnectionSettings) -> AsyncpgConnectKwargs:
    """Connect kwargs for asyncpg; keys are absent unless explicitly set.

    Connect timeout ladder: PGQUEUER_CONNECT_TIMEOUT > PGCONNECT_TIMEOUT >
    driver default. asyncpg does not read ``PGCONNECT_TIMEOUT`` itself (real
    libpq does), so the fallback lives here, on the asyncpg path only.
    """
    kwargs: AsyncpgConnectKwargs = {}
    timeout = settings.connect_timeout
    if timeout is None and (env := os.environ.get("PGCONNECT_TIMEOUT")):
        timeout = float(env)
    if timeout is not None:
        kwargs["timeout"] = timeout
    if settings.application_name is not None:
        kwargs["server_settings"] = {"application_name": settings.application_name}
    return kwargs


def _psycopg_connect_kwargs(settings: ConnectionSettings) -> PsycopgConnectKwargs:
    """Connect kwargs for psycopg pool connections.

    ``autocommit=True`` is mandatory: ``PsycopgDriver`` requires it. It is a
    client-side psycopg attribute, not a conninfo parameter, so there is no
    DSN-clobbering risk. libpq's ``connect_timeout`` is an integer; round up
    so the effective timeout is never shorter than requested.
    """
    kwargs: PsycopgConnectKwargs = {"autocommit": True}
    if settings.connect_timeout is not None:
        kwargs["connect_timeout"] = math.ceil(settings.connect_timeout)
    if settings.application_name is not None:
        kwargs["application_name"] = settings.application_name
    return kwargs


@asynccontextmanager
async def connect_asyncpg(
    dsn: str | None = None,
    settings: ConnectionSettings | None = None,
) -> AsyncIterator[asyncpg.Connection]:
    """Open a single asyncpg connection honoring ``ConnectionSettings``."""
    import asyncpg

    dsn, settings = _resolve(dsn, settings)
    connection = await asyncpg.connect(dsn=dsn, **_asyncpg_connect_kwargs(settings))
    try:
        yield connection
    finally:
        await connection.close()


@asynccontextmanager
async def connect_psycopg(
    dsn: str | None = None,
    settings: ConnectionSettings | None = None,
) -> AsyncIterator[psycopg.AsyncConnection]:
    """Open a single psycopg connection honoring ``ConnectionSettings``."""
    import psycopg

    dsn, settings = _resolve(dsn, settings)
    async with await psycopg.AsyncConnection.connect(
        dsn or "",
        **_psycopg_connect_kwargs(settings),
    ) as connection:
        yield connection


@asynccontextmanager
async def create_asyncpg_pool(
    dsn: str | None = None,
    settings: ConnectionSettings | None = None,
) -> AsyncIterator[asyncpg.Pool]:
    """Create an asyncpg pool sized/configured by ``ConnectionSettings``."""
    import asyncpg

    dsn, settings = _resolve(dsn, settings)
    async with asyncpg.create_pool(
        dsn=dsn,
        min_size=settings.pool_min_size,
        max_size=settings.pool_max_size,
        **_asyncpg_connect_kwargs(settings),
    ) as pool:
        yield pool


@asynccontextmanager
async def create_psycopg_pool(
    dsn: str | None = None,
    settings: ConnectionSettings | None = None,
) -> AsyncIterator[psycopg_pool.AsyncConnectionPool]:
    """Create a psycopg async pool sized/configured by ``ConnectionSettings``."""
    import psycopg_pool

    dsn, settings = _resolve(dsn, settings)
    async with psycopg_pool.AsyncConnectionPool(
        conninfo=dsn or "",
        min_size=settings.pool_min_size,
        max_size=settings.pool_max_size,
        kwargs=dict(_psycopg_connect_kwargs(settings)),
        open=False,
    ) as pool:
        yield pool
