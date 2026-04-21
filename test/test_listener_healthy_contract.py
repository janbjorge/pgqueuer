from __future__ import annotations

from datetime import timedelta

import pytest

from pgqueuer import db
from pgqueuer.domain.errors import FailingListenerError
from pgqueuer.qm import QueueManager
from pgqueuer.queries import Queries


async def test_listener_healthy_raises_on_timeout(apgdriver: db.Driver) -> None:
    """listener_healthy must raise FailingListenerError directly on timeout."""
    qm = QueueManager(Queries(apgdriver))

    # No listener set up, so the health check notification will never arrive.
    with pytest.raises(FailingListenerError):
        await qm.listener_healthy(timeout=timedelta(milliseconds=100))
