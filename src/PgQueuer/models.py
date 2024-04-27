from typing import Literal

from pydantic import AwareDatetime, BaseModel

STATUS = Literal[
    "queued",
    "picked",
]
STATUS_LOG = Literal[
    "exception",
    "successful",
]


class Job(BaseModel):
    """
    Represents a job with attributes such as ID, priority,
    creation time, status, entrypoint, and optional payload.
    """

    id: int
    priority: int
    created: AwareDatetime
    status: STATUS
    entrypoint: str
    payload: bytes | None
