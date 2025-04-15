import json
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import asyncpg
from fastapi import Depends, FastAPI, Request, Response

from pgqueuer.db import AsyncpgDriver
from pgqueuer.queries import Queries


def get_pgq_queries(request: Request) -> Queries:
    """Retrieve Queries instance from FastAPI app context."""
    return request.app.extra["pgq_queries"]


def create_app() -> FastAPI:
    """
    Create and configure a FastAPI app with a lifespan context manager
    to handle database connection.
    """

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
        """Manage async database connection throughout the app's lifespan."""
        connection = await asyncpg.connect()
        app.extra["pgq_queries"] = Queries(AsyncpgDriver(connection))
        try:
            yield
        finally:
            await connection.close()

    app = FastAPI(lifespan=lifespan)

    @app.get("/reset_password_email")
    async def reset_password_email(
        user_name: str,
        pgq_queries: Queries = Depends(get_pgq_queries),
    ) -> Response:
        """Enqueue a job to reset a user's password, identified by user_name."""
        await pgq_queries.enqueue(
            "reset_email_by_user_name",
            payload=json.dumps({"user_name": user_name}).encode(),
        )
        return Response(status_code=201)

    return app


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(create_app())
