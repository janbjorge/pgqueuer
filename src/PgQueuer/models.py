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


class QueueSize(BaseModel):
    """
    Represents the number of jobs per entrypoint and priority in the queue.
    """

    count: int
    entrypoint: str
    priority: int


class LogSize(BaseModel):
    """
    Represents log details for jobs based on status, entrypoint, and priority.
    """

    count: int
    entrypoint: str
    priority: int
    status: STATUS_LOG
