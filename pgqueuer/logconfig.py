"""
Logging configuration for the pgqueuer application.

This module initializes the logging settings for the application. It sets up a logger
named 'pgqueuer' and configures its log level based on the 'LOGLEVEL' environment variable.
If 'LOGLEVEL' is not set, it defaults to 'INFO'.
"""

from __future__ import annotations

import logging
from typing import Final

logger: Final = logging.getLogger("pgqueuer")
