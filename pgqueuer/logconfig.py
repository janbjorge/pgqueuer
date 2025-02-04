"""
Logging configuration for the pgqueuer application.

This module initializes the logging settings for the application. It sets up a logger
named 'pgqueuer' and configures its log level based on the 'LOGLEVEL' environment variable.
If 'LOGLEVEL' is not set, it defaults to 'INFO'.
"""

from __future__ import annotations

import logging
import logging.config
import os
import sys
from datetime import datetime
from typing import Final


class ISOFormatter(logging.Formatter):
    """Formatter that outputs log timestamps in ISO 8601 format with timezone information."""

    def formatTime(self, record, datefmt=None):
        """Return the creation time of the record as an ISO 8601 formatted string with timezone.

        Args:
            record (logging.LogRecord): The log record containing the creation timestamp.
            datefmt (str, optional): Ignored. Kept for compatibility.

        Returns:
            str: ISO 8601 formatted timestamp.
        """
        return datetime.fromtimestamp(record.created).astimezone().isoformat()


class MaxLevelFilter(logging.Filter):
    """Filter that only allows log records up to a specified maximum logging level."""

    def __init__(self, max_level):
        """Initialize the filter.

        Args:
            max_level (int): The maximum log level (inclusive) to allow.
        """
        super().__init__()
        self.max_level = max_level

    def filter(self, record):
        """Determine if the specified record is to be logged.

        Args:
            record (logging.LogRecord): The log record to be evaluated.

        Returns:
            bool: True if record.levelno is less than or equal to max_level, False otherwise.
        """
        return record.levelno <= self.max_level


LOGGING_CONFIG: Final = {
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
    "root": {
        "handlers": ["stdout", "stderr"],
        "level": "DEBUG",
    },
}


logger: Final = logging.getLogger("pgqueuer")
logger.addHandler(logging.NullHandler())
if "PGQUEUER_DISABLE_LOGGING" not in os.environ:
    logging.config.dictConfig(LOGGING_CONFIG)
