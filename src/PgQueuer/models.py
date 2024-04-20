from typing import Literal

from pydantic import AwareDatetime, BaseModel, RootModel

STATUS = Literal[
    "queue",
    "picked",
]
STATUS_LOG = Literal[
    "exception",
    "successful",
]


class Job(BaseModel):
    id: int
    priority: int
    created: AwareDatetime
    status: STATUS
    entrypoint: str
    payload: bytes | None


class Jobs(RootModel[list[Job]]): ...
