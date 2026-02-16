"""Backward-compatibility shim. Canonical: pgqueuer.adapters.persistence.qb"""
from pgqueuer.adapters.persistence.qb import (
    DBSettings,
    Durability,
    DurabilityPolicy,
    QueryBuilderEnvironment,
    QueryQueueBuilder,
    QuerySchedulerBuilder,
    add_prefix,
)

__all__ = [
    "DBSettings",
    "Durability",
    "DurabilityPolicy",
    "QueryBuilderEnvironment",
    "QueryQueueBuilder",
    "QuerySchedulerBuilder",
    "add_prefix",
]
