"""Django-backed drivers and connection-parameter derivation.

Django integration needs two distinct connections:

* **Enqueue** borrows Django's own connection, so inserts join the caller's
  ``transaction.atomic()`` block.
* **Worker** needs a native connection, because Django's connection cannot hold a
  long-lived ``LISTEN`` (Django has no async database layer).

:class:`DjangoDriver` covers the first; :func:`worker_connection_params` derives
the parameters for the second from Django's own configuration.
"""

from __future__ import annotations

from typing import Any

import psycopg
from django.db import connections
from psycopg.rows import dict_row

from pgqueuer.ports.driver import SyncDriver

# Keys Django's psycopg backend adds that psycopg.connect() itself rejects.
_UNSUPPORTED_CONNECT_KEYS = ("cursor_factory", "context")


def worker_connection_params(alias: str = "default") -> dict[str, Any]:
    """Return ``psycopg.connect()`` kwargs for *alias*, derived from Django.

    Asking Django's backend rather than parsing ``settings.DATABASES`` inherits
    ``NAME`` -> ``dbname`` mapping, ``OPTIONS`` passthrough and — importantly for
    tests — Django's test-database name substitution.

    Usage example::

        params = worker_connection_params()
        connection = await psycopg.AsyncConnection.connect(**params, autocommit=True)
    """
    params = dict(connections[alias].get_connection_params())
    for key in _UNSUPPORTED_CONNECT_KEYS:
        params.pop(key, None)
    return params


class DjangoDriver(SyncDriver):
    """Synchronous driver executing on Django's connection for *alias*.

    The connection is looked up per call rather than cached, so the driver
    follows Django's thread-local wrapper across reconnects. Unlike
    :class:`~pgqueuer.adapters.drivers.psycopg.SyncPsycopgDriver` it does not
    require autocommit, which is what allows enqueueing inside
    ``transaction.atomic()``.

    Usage example::

        queries = SyncQueries(DjangoDriver())
        with transaction.atomic():
            order.save()
            queries.enqueue("send-invoice", payload)
    """

    def __init__(self, alias: str = "default") -> None:
        self.alias = alias

    @property
    def connection(self) -> psycopg.Connection:
        wrapper = connections[self.alias]
        wrapper.ensure_connection()
        return wrapper.connection

    def fetch(
        self,
        query: str,
        *args: Any,
    ) -> list[dict]:
        cursor = psycopg.RawCursor(self.connection, row_factory=dict_row)
        cursor.execute(query, args or None)
        return cursor.fetchall()
