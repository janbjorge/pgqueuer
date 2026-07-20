"""Domain configuration types for PgQueuer."""

from __future__ import annotations

import dataclasses
import functools
import os
import re
import warnings
from enum import Enum
from typing import Callable, Literal

from pydantic import AliasChoices, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from pgqueuer.domain.models import Channel

IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def add_prefix(string: str) -> str:
    """Deprecated: ``DBSettings`` applies ``PGQUEUER_PREFIX`` itself via its ``prefix`` field."""
    warnings.warn(
        "add_prefix is deprecated; DBSettings reads PGQUEUER_PREFIX itself "
        "(DBSettings(prefix=...) or the PGQUEUER_PREFIX environment variable).",
        DeprecationWarning,
        stacklevel=2,
    )
    return f"{os.environ.get('PGQUEUER_PREFIX', '')}{string}"


def name_env_alias(field_name: str) -> AliasChoices:
    """Env spellings for an object-name field: ``PGQUEUER_X`` plus the legacy bare ``X``."""
    return AliasChoices(f"pgqueuer_{field_name}", field_name)


def prefixed_default(base: str) -> Callable[[dict[str, str]], str]:
    """Default for an object-name field: *base* behind the already-validated ``prefix``.

    Runs only when the field is not explicitly provided, so overrides are
    never double-prefixed. Requires ``prefix`` to be declared before the name
    fields (pydantic passes previously validated fields as ``data``).
    """
    return lambda data: f"{data['prefix']}{base}"


@dataclasses.dataclass(frozen=True)
class DurabilityPolicy:
    """Per-table persistence: ``""`` = LOGGED (WAL); ``"UNLOGGED"`` = faster, crash-lossy."""

    queue_table: Literal["", "UNLOGGED"]
    queue_log_table: Literal["", "UNLOGGED"]
    statistics_table: Literal["", "UNLOGGED"]
    schedules_table: Literal["", "UNLOGGED"]


class Durability(Enum):
    """
    Represents the durability levels for PgQueuer table installations.

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


class ConnectionSettings(BaseSettings):
    """Connection/pool parameters, read from ``PGQUEUER_*`` env vars.

    Unset optional fields are never passed to the driver, so DSN parameters
    and libpq environment variables keep full control of connection
    behavior (pass-through principle, #605/#701).
    """

    model_config = SettingsConfigDict(
        env_prefix="PGQUEUER_",
        extra="ignore",
        populate_by_name=True,
    )

    dsn: str | None = Field(
        default=None,
        validation_alias=AliasChoices("PGQUEUER_DSN", "PGDSN"),
    )
    # Size sanity (max >= min) is enforced by the drivers themselves.
    pool_min_size: int = Field(default=1, ge=0)
    pool_max_size: int = Field(default=5, ge=1)
    connect_timeout: float | None = Field(default=None, gt=0)
    application_name: str | None = Field(default=None)


@dataclasses.dataclass(frozen=True)
class QualifiedNames:
    """Schema-qualified spellings of the schema-scoped DB objects.

    Deliberately omits ``channel`` (NOTIFY channels are database-global),
    ``trigger`` (trigger names are referenced unqualified, scoped by their
    table) and index names (unqualified, they inherit the table's schema).
    """

    queue_table: str
    queue_table_log: str
    statistics_table: str
    schedules_table: str
    function: str
    queue_status_type: str


class DBSettings(BaseSettings):
    """Names of PgQueuer DB objects, namespaced by ``prefix`` and placed in ``db_schema``.

    Each name field accepts two env spellings: the ``PGQUEUER_``-prefixed one
    (preferred) and the legacy bare one that predates it (e.g.
    ``PGQUEUER_QUEUE_TABLE`` and ``QUEUE_TABLE``).
    """

    model_config = SettingsConfigDict(
        env_prefix="PGQUEUER_",
        extra="ignore",
    )

    # Prepended to every object name not explicitly overridden; lets multiple
    # PgQueuer instances share one database. Declared first: the name-field
    # default factories read it from the validated data.
    prefix: str = Field(default="")

    # Postgres schema holding all PgQueuer objects; None defers to the
    # connection's search_path (env var: PGQUEUER_SCHEMA).
    db_schema: str | None = Field(
        default=None,
        validation_alias=AliasChoices("db_schema", "pgqueuer_schema"),
    )

    channel: Channel = Field(
        default_factory=lambda data: Channel(f"{data['prefix']}ch_pgqueuer"),
        validation_alias=name_env_alias("channel"),
    )
    function: str = Field(
        default_factory=prefixed_default("fn_pgqueuer_changed"),
        validation_alias=name_env_alias("function"),
    )
    statistics_table: str = Field(
        default_factory=prefixed_default("pgqueuer_statistics"),
        validation_alias=name_env_alias("statistics_table"),
    )
    queue_status_type: str = Field(
        default_factory=prefixed_default("pgqueuer_status"),
        validation_alias=name_env_alias("queue_status_type"),
    )
    queue_table: str = Field(
        default_factory=prefixed_default("pgqueuer"),
        validation_alias=name_env_alias("queue_table"),
    )
    queue_table_log: str = Field(
        default_factory=prefixed_default("pgqueuer_log"),
        validation_alias=name_env_alias("queue_table_log"),
    )
    trigger: str = Field(
        default_factory=prefixed_default("tg_pgqueuer_changed"),
        validation_alias=name_env_alias("trigger"),
    )
    schedules_table: str = Field(
        default_factory=prefixed_default("pgqueuer_schedules"),
        validation_alias=name_env_alias("schedules_table"),
    )
    durability: Durability = Field(
        default=Durability.durable, validation_alias=name_env_alias("durability")
    )

    # When True, `pgq upgrade` widens legacy int4 id columns/sequences to BIGINT
    # (issue #671). The widen takes an ACCESS EXCLUSIVE lock and rewrites each
    # table; set False to skip it and apply the widening out-of-band.
    widen_id: bool = Field(default=True, validation_alias=name_env_alias("widen_id"))

    @field_validator("prefix")
    @classmethod
    def validate_prefix(cls, value: str) -> str:
        if value and not IDENTIFIER_PATTERN.match(value):
            raise ValueError(
                "prefix must start with a letter or underscore and contain only "
                "letters, digits, and underscores"
            )
        return value

    @field_validator("db_schema")
    @classmethod
    def validate_db_schema(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if not IDENTIFIER_PATTERN.match(value):
            raise ValueError(
                "db_schema must be a single unquoted identifier (letters, digits, "
                "underscores; no dots)"
            )
        # Rendered both as an unquoted identifier (folds to lowercase) and as a
        # nspname string literal (does not fold); normalize so both agree.
        return value.lower()

    @model_validator(mode="after")
    def reject_dotted_names_with_db_schema(self) -> DBSettings:
        """A pre-qualified name plus db_schema would render ``schema.a.b``; refuse the mix."""
        if not self.db_schema:
            return self
        for name in (
            self.function,
            self.statistics_table,
            self.queue_status_type,
            self.queue_table,
            self.queue_table_log,
            self.schedules_table,
        ):
            if "." in name:
                raise ValueError(
                    f"object name {name!r} already contains a schema qualifier; "
                    "drop the dot or unset db_schema/PGQUEUER_SCHEMA"
                )
        return self

    @property
    def legacy_statistics_status_type(self) -> str:
        """Pre-v0.27 enum type name, kept only so uninstall can drop it."""
        return f"{self.prefix}pgqueuer_statistics_status"

    def qualify(self, name: str) -> str:
        """Schema-qualified reference for *name*; bare when no db_schema is set."""
        return f"{self.db_schema}.{name}" if self.db_schema else name

    # cached_property: query builders access this on every SQL render (the
    # dequeue loop rebuilds its query each iteration). Safe because names are
    # fixed once validation and apply_prefix have run.
    @functools.cached_property
    def qualified(self) -> QualifiedNames:
        return QualifiedNames(
            queue_table=self.qualify(self.queue_table),
            queue_table_log=self.qualify(self.queue_table_log),
            statistics_table=self.qualify(self.statistics_table),
            schedules_table=self.qualify(self.schedules_table),
            function=self.qualify(self.function),
            queue_status_type=self.qualify(self.queue_status_type),
        )

    @property
    def schema_expr(self) -> str:
        """SQL expression selecting the effective schema in catalog queries.

        Interpolated literal when db_schema is set (safe: identifier-validated,
        lowercase-normalized); current_schema() otherwise, and usable inside
        DO-blocks which cannot take bind parameters.
        """
        return f"'{self.db_schema}'" if self.db_schema else "current_schema()"
