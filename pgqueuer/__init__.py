from pgqueuer.adapters.persistence.inmemory import InMemoryRepository
from pgqueuer.applications import PgQueuer
from pgqueuer.db import AsyncpgDriver, AsyncpgPoolDriver, PsycopgDriver
from pgqueuer.models import Job, JobId
from pgqueuer.qm import QueueManager
from pgqueuer.queries import Queries
from pgqueuer.sm import SchedulerManager

try:
    from pgqueuer._version import version as __version__
except ImportError:
    __version__ = "0.0.0"  # Fallback version if _version.py is missing

__all__ = [
    "AsyncpgDriver",
    "AsyncpgPoolDriver",
    "InMemoryRepository",
    "Job",
    "JobId",
    "PgQueuer",
    "PsycopgDriver",
    "Queries",
    "QueueManager",
    "SchedulerManager",
]
