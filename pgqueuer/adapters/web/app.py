from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

try:
    from fastapi import Depends, FastAPI
except ImportError as e:
    raise ImportError(
        "fastapi is required for this module. Install with: pip install pgqueuer[web]"
    ) from e

from pgqueuer.adapters.persistence import qb, queries
from pgqueuer.adapters.web.auth import PASSWORD_ENV, USER_ENV, create_basic_auth_dependency
from pgqueuer.adapters.web.routes import create_web_router
from pgqueuer.adapters.web.sse import Broadcaster
from pgqueuer.core import logconfig
from pgqueuer.domain.settings import ConnectionSettings


def create_web_app(
    dsn: str | None = None,
    connection_settings: ConnectionSettings | None = None,
) -> FastAPI:
    """Standalone dashboard app: asyncpg pool, NOTIFY-driven SSE, optional Basic auth.

    Usage example::

        import uvicorn
        from pgqueuer.web import create_web_app

        uvicorn.run(create_web_app(), host="0.0.0.0", port=8080)

    When *dsn* is None, the DSN comes from PGQUEUER_DSN/PGDSN, else asyncpg
    reads standard libpq env vars (PGHOST, PGUSER, ...). Pool sizing and
    timeouts come from *connection_settings*, or PGQUEUER_* env vars when None.
    Auth is enabled only when PGQUEUER_WEB_USER and PGQUEUER_WEB_PASSWORD are set.
    """

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        from pgqueuer.adapters.connections import create_asyncpg_pool
        from pgqueuer.adapters.drivers.asyncpg import AsyncpgPoolDriver

        settings = qb.DBSettings()
        async with create_asyncpg_pool(dsn=dsn, settings=connection_settings) as pool:
            driver = AsyncpgPoolDriver(pool)
            app.state.pgq_queries = queries.Queries(
                driver,
                qbe=qb.QueryBuilderEnvironment(settings),
                qbq=qb.QueryQueueBuilder(settings),
                qbs=qb.QuerySchedulerBuilder(settings),
            )
            broadcaster = Broadcaster(driver=driver, channel=settings.channel)
            await broadcaster.start()
            app.state.pgq_broadcaster = broadcaster
            async with driver:
                yield

    auth = create_basic_auth_dependency()
    if auth is None:
        logconfig.logger.warning(
            "The pgqueuer web dashboard is running WITHOUT authentication. "
            "Anyone who can reach it can browse the queue and mutate it via "
            "POST /jobs/{id}/cancel and POST /jobs/requeue. "
            "Set %s and %s to enable HTTP Basic auth.",
            USER_ENV,
            PASSWORD_ENV,
        )

    app = FastAPI(title="pgqueuer dashboard", lifespan=lifespan)
    app.include_router(
        create_web_router(dependencies=[Depends(auth)] if auth else None),
    )
    return app
