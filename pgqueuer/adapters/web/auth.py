from __future__ import annotations

import os
import secrets
from typing import Callable

try:
    from fastapi import Depends, HTTPException
    from fastapi.security import HTTPBasic, HTTPBasicCredentials
except ImportError as e:
    raise ImportError(
        "fastapi is required for this module. Install with: pip install pgqueuer[web]"
    ) from e

USER_ENV = "PGQUEUER_WEB_USER"
PASSWORD_ENV = "PGQUEUER_WEB_PASSWORD"


def create_basic_auth_dependency() -> Callable[..., None] | None:
    """HTTP Basic auth dependency from env vars, or None when auth is not configured."""
    user = os.environ.get(USER_ENV)
    password = os.environ.get(PASSWORD_ENV)
    if not user or not password:
        return None

    security = HTTPBasic()

    def verify(credentials: HTTPBasicCredentials = Depends(security)) -> None:
        username_ok = secrets.compare_digest(credentials.username.encode(), user.encode())
        password_ok = secrets.compare_digest(credentials.password.encode(), password.encode())
        if not (username_ok and password_ok):
            raise HTTPException(
                status_code=401,
                detail="Invalid credentials",
                headers={"WWW-Authenticate": "Basic"},
            )

    return verify
