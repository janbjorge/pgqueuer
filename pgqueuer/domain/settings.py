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
    """
    Append a prefix from environment variables to a given string.

    This function prepends the value of the 'PGQUEUER_PREFIX' environment variable
    to the provided string. It is typically used to add a consistent prefix to
    database object names (e.g., tables, triggers) to avoid naming conflicts
    or to namespace the objects.

    Args:
        string (str): The base string to which the prefix will be added.

    Returns:
        str: The string with the prefix appended.
    """

    env = os.environ.get("PGQUEUER_PREFIX", "")
    # - Starts with a letter or underscore
    # - Contains only letters, numbers, and underscores
    # - No dots or special characters
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
    """
    Defines the logging configuration for PGQueuer database tables.

    Attributes:
        pgqueuer (Literal['', 'UNLOGGED']):
            Logging configuration for the `pgqueuer` table.
            - '' (empty string): The table is logged.
            - 'UNLOGGED': The table is unlogged for performance optimization.

        pgqueuer_log (Literal['', 'UNLOGGED']):
            Logging configuration for the `pgqueuer_log` table.
            - '' (empty string): The table is logged.
            - 'UNLOGGED': The table is unlogged for performance optimization.

        pgqueuer_statistics (Literal['', 'UNLOGGED']):
            Logging configuration for the `pgqueuer_statistics` table.
            - '' (empty string): The table is logged.
            - 'UNLOGGED': The table is unlogged for performance optimization.

        pgqueuer_schedules (Literal['', 'UNLOGGED']):
            Logging configuration for the `pgqueuer_schedules` table.
            Matches the configuration of the `pgqueuer` table.
            - '' (empty string): The table is logged.
            - 'UNLOGGED': The table is unlogged for performance optimization.
    """

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
        """
        Returns the `DurabilityPolicy` associated with the durability level.

        Returns:
            DurabilityPolicy: A configuration object specifying the logging mode for each table.

        Logging Modes:
            - '' (empty string): Indicates the table is logged.
            - 'UNLOGGED': Indicates the table is unlogged for performance optimization.
        """
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
    """
    Configuration settings for database object names with optional prefixes.

    This class contains the names of various database objects used by the job queue
    system, such as tables, functions, triggers, channels, and scheduler tables. The
    settings allow for the generation of object names with configurable prefixes,
    which are set via environment variables. This is useful for avoiding naming
    conflicts and supporting multiple instances with different namespaces.

    """

    model_config = SettingsConfigDict(
        env_prefix=add_prefix(""),
        extra="ignore",
    )

    # Channel name for PostgreSQL LISTEN/NOTIFY used to
    # receive notifications about changes in the queue.
    channel: Channel = Field(default=Channel(add_prefix("ch_pgqueuer")))

    # Name of the database function triggered by changes to the queue
    # table, used to notify subscribers.
    function: str = Field(default=add_prefix("fn_pgqueuer_changed"))

    # Name of the table that logs statistics about job processing,
    # e.g., processing times and outcomes.
    statistics_table: str = Field(default=add_prefix("pgqueuer_statistics"))

    # Type of ENUM defining possible statuses for entries in the
    # statistics table, such as 'exception' or 'successful'.
    # TODO: Remove in future release
    statistics_table_status_type: str = Field(default=add_prefix("pgqueuer_statistics_status"))

    # Type of ENUM defining statuses for queue jobs, such as 'queued' or 'picked'.
    queue_status_type: str = Field(default=add_prefix("pgqueuer_status"))

    # Name of the main table where jobs are queued before being processed.
    queue_table: str = Field(default=add_prefix("pgqueuer"))

    # Name of the pgqueuer log table (log of `queue_table`).
    queue_table_log: str = Field(default=add_prefix("pgqueuer_log"))

    # Name of the trigger that invokes the function to notify changes, applied
    # after DML operations on the queue table.
    trigger: str = Field(default=add_prefix("tg_pgqueuer_changed"))

    # Name of scheduler table
    schedules_table: str = Field(default=add_prefix("pgqueuer_schedules"))

    # Specifies the durability policy for the database schema.
    durability: Durability = Field(default=Durability.durable)
