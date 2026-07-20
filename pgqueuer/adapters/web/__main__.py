"""Run the dashboard standalone: ``python -m pgqueuer.adapters.web``."""

from __future__ import annotations

import os

import uvicorn

from pgqueuer.adapters.web.app import create_web_app


def main() -> None:
    # DSN resolution (PGQUEUER_DSN/PGDSN, then libpq env vars) happens
    # inside create_web_app via ConnectionSettings.
    uvicorn.run(
        create_web_app(),
        host=os.environ.get("PGQUEUER_WEB_HOST", "127.0.0.1"),
        port=int(os.environ.get("PGQUEUER_WEB_PORT", "8080")),
    )


if __name__ == "__main__":
    main()
