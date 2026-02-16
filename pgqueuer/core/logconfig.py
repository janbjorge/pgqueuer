"""
Logging configuration for the pgqueuer application.
"""

from __future__ import annotations

import logging
import logging.config
import sys
from datetime import datetime
from enum import Enum
from typing import Final


class LogLevel(Enum):
    CRITICAL = "CRITICAL"
    FATAL = "FATAL"
    ERROR = "ERROR"
    WARN = "WARN"
    WARNING = "WARNING"
    INFO = "INFO"
    DEBUG = "DEBUG"
    NOTSET = "NOTSET"


class ISOFormatter(logging.Formatter):
    """Outputs log timestamps in ISO 8601 format with timezone information."""

    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        return datetime.fromtimestamp(record.created).astimezone().isoformat()


class MaxLevelFilter(logging.Filter):
    """Only allows log records up to a specified maximum logging level."""

    def __init__(self, max_level: int):
        super().__init__()
        self.max_level = max_level

    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno <= self.max_level


def setup_fancy_logger(level: LogLevel) -> None:
    """
    Update the 'pgqueuer' logger's level dynamically.
    """

    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "default": {
                    "()": ISOFormatter,
                    "format": "%(asctime)s [%(name)s] %(levelname)s: %(message)s",
                },
            },
            "filters": {
                "max_info": {
                    "()": MaxLevelFilter,
                    "max_level": logging.INFO,
                },
            },
            "handlers": {
                "stdout": {
                    "class": "logging.StreamHandler",
                    "level": "DEBUG",
                    "formatter": "default",
                    "stream": sys.stdout,
                    "filters": ["max_info"],
                },
                "stderr": {
                    "class": "logging.StreamHandler",
                    "level": "WARNING",
                    "formatter": "default",
                    "stream": sys.stderr,
                },
            },
            "loggers": {
                "pgqueuer": {
                    "handlers": ["stdout", "stderr"],
                    "level": level.name,
                }
            },
        }
    )


# Create the 'pgqueuer' logger and attach a NullHandler by default.
logger: Final = logging.getLogger("pgqueuer")
logger.addHandler(logging.NullHandler())
