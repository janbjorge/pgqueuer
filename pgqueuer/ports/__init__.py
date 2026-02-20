from __future__ import annotations

from typing import Protocol

from pgqueuer.ports.driver import Driver, SyncDriver
from pgqueuer.ports.repository import (
    NotificationPort,
    QueueRepositoryPort,
    ScheduleRepositoryPort,
    SchemaManagementPort,
)
from pgqueuer.ports.tracing import TracingProtocol


class RepositoryPort(QueueRepositoryPort, ScheduleRepositoryPort, NotificationPort, SchemaManagementPort, Protocol):
    """Combined repository protocol for drop-in adapter implementations."""

    pass

__all__ = [
    "Driver",
    "NotificationPort",
    "QueueRepositoryPort",
    "RepositoryPort",
    "ScheduleRepositoryPort",
    "SchemaManagementPort",
    "SyncDriver",
    "TracingProtocol",
]
