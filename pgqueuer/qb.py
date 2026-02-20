"""Backward-compatibility shim. Canonical locations:
- Settings types: pgqueuer.domain.settings
- SQL builders: pgqueuer.adapters.persistence.qb
"""

from pgqueuer.adapters.persistence.qb import (
    QueryBuilderEnvironment,
    QueryQueueBuilder,
    QuerySchedulerBuilder,
)
from pgqueuer.domain.settings import (
    DBSettings,
    Durability,
    DurabilityPolicy,
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
