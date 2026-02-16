"""Backward-compatibility shim. Canonical: pgqueuer.core.logconfig"""

from pgqueuer.core.logconfig import (
    ISOFormatter,
    LogLevel,
    MaxLevelFilter,
    logger,
    setup_fancy_logger,
)

__all__ = [
    "ISOFormatter",
    "LogLevel",
    "MaxLevelFilter",
    "logger",
    "setup_fancy_logger",
]
