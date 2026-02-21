"""In-memory adapter — drop-in replacement for the PostgreSQL backend."""

from pgqueuer.adapters.inmemory.driver import InMemoryDriver
from pgqueuer.adapters.inmemory.queries import InMemoryQueries

__all__ = [
    "InMemoryDriver",
    "InMemoryQueries",
]
