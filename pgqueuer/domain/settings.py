"""Domain configuration types for PgQueuer.

These are pure configuration / value objects with no adapter-specific
dependencies, so they belong in the domain layer.
"""

from __future__ import annotations

import dataclasses
import os
import re
from enum import Enum
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from pgqueuer.domain.models import Channel


def add_prefix(string: str) -> str:
    """Prepend ``PGQUEUER_PREFIX`` (if set) to *string* for namespacing DB objects."""
    env = os.environ.get("PGQUEUER_PREFIX", "")
    if env and not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", env):
        raise ValueError(
            "Invalid prefix: The 'PGQUEUER_PREFIX' environment variable must "
            "start with a letter or underscore and contain only letters, "
            "numbers, and underscores. It cannot contain '.', spaces, or "
            "other special characters."
        )

    return f"{env}{string}"


@dataclasses.dataclass(frozen=True)
class DurabilityPolicy:
    """Per-table persistence: ``""`` = LOGGED (WAL); ``"UNLOGGED"`` = faster, crash-lossy."""

    queue_table: Literal["", "UNLOGGED"]
    queue_log_table: Literal["", "UNLOGGED"]
    statistics_table: Literal["", "UNLOGGED"]
    schedules_table: Literal["", "UNLOGGED"]


class Durability(Enum):
    """
    Represents the durability levels for PGQueuer table installations.

    Each durability level corresponds to a specific `DurabilityPolicy` instance that defines
    the logging configuration for all database tables.

    Levels:
        - `volatile`: All tables are unlogged, prioritizing maximum performance
          over data durability. Suitable for temporary or ephemeral queue data where
          data loss in the event of a crash is acceptable.

        - `balanced`: The `pgqueuer` and `pgqueuer_schedules` tables are logged, ensuring durability
          for critical data, while auxiliary tables (`pgqueuer_log` and `pgqueuer_statistics`) are
          unlogged to optimize performance.

        - `durable`: All tables are logged, ensuring maximum data durability and safety.
          This is ideal for production environments where data integrity is critical.
    """

    volatile = "volatile"
    balanced = "balanced"
    durable = "durable"

    @property
    def config(self) -> DurabilityPolicy:
        """Per-table LOGGED/UNLOGGED policy for this durability level."""
        match self:
            case Durability.volatile:
                return DurabilityPolicy(
                    queue_table="UNLOGGED",
                    queue_log_table="UNLOGGED",
                    statistics_table="UNLOGGED",
                    schedules_table="UNLOGGED",  # Matches `pgqueuer`
                )
            case Durability.balanced:
                return DurabilityPolicy(
                    queue_table="",
                    queue_log_table="UNLOGGED",
                    statistics_table="UNLOGGED",
                    schedules_table="",  # Matches `pgqueuer`
                )
            case Durability.durable:
                return DurabilityPolicy(
                    queue_table="",
                    queue_log_table="",
                    statistics_table="",
                    schedules_table="",  # Matches `pgqueuer`
                )
            case _:
                raise ValueError(f"Unknown durability level: {self}")


class DBSettings(BaseSettings):
    """Names of PgQueuer DB objects, each prefixed via ``PGQUEUER_PREFIX``."""

    model_config = SettingsConfigDict(
        env_prefix=add_prefix(""),
        extra="ignore",
    )

    channel: Channel = Field(default=Channel(add_prefix("ch_pgqueuer")))
    function: str = Field(default=add_prefix("fn_pgqueuer_changed"))
    statistics_table: str = Field(default=add_prefix("pgqueuer_statistics"))
    queue_status_type: str = Field(default=add_prefix("pgqueuer_status"))
    queue_table: str = Field(default=add_prefix("pgqueuer"))
    queue_table_log: str = Field(default=add_prefix("pgqueuer_log"))
    trigger: str = Field(default=add_prefix("tg_pgqueuer_changed"))
    schedules_table: str = Field(default=add_prefix("pgqueuer_schedules"))
    durability: Durability = Field(default=Durability.durable)
