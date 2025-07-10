from __future__ import annotations

import contextlib
from dataclasses import dataclass, field
from typing import Any, Iterator

from . import models


@dataclass
class Telemetry:
    """Lazy Sentry instrumentation for queue operations."""

    _sdk: Any | None = field(default=None, init=False, repr=False)

    def _load_sdk(self) -> bool:
        if self._sdk is not None:
            return self._sdk is not False
        try:
            import sentry_sdk
        except ModuleNotFoundError:
            self._sdk = False
            return False
        else:
            self._sdk = sentry_sdk
            return True

    def trace_headers(self) -> dict[str, str]:
        """Return trace headers for propagation."""
        if not self._load_sdk():
            return {}
        assert self._sdk
        return {
            "sentry-trace": self._sdk.get_traceparent(),
            "baggage": self._sdk.get_baggage(),
        }

    @contextlib.contextmanager
    def publish_span(self, queue: str, message_id: str, size: int) -> Iterator[None]:
        """Trace queue publish events."""
        if not self._load_sdk():
            yield
        else:
            assert self._sdk
            with self._sdk.start_span(
                op="queue.publish",
                description=queue,
            ) as span:
                span.set_data("messaging.message.id", message_id)
                span.set_data("messaging.destination.name", queue)
                span.set_data("messaging.message.body.size", size)
                yield

    @contextlib.contextmanager
    def job_span(self, job: models.Job, retry_count: int = 0) -> Iterator[None]:
        """Context manager tracing the execution of ``job``."""
        if not self._load_sdk():
            yield
        else:
            assert self._sdk
            with self._sdk.start_span(
                op="queue.process",
                description=job.entrypoint,
            ) as span:
                span.set_data("messaging.message.id", int(job.id))
                span.set_data("messaging.destination.name", job.entrypoint)
                span.set_data(
                    "messaging.message.body.size",
                    len(job.payload or b""),
                )
                span.set_data("messaging.message.retry.count", retry_count)
                yield

    @contextlib.contextmanager
    def schedule_span(self, schedule: models.Schedule) -> Iterator[None]:
        """Context manager tracing the execution of ``schedule``."""
        if not self._load_sdk():
            yield
        else:
            assert self._sdk
            with self._sdk.start_span(
                op="pgqueuer.schedule",
                description=schedule.entrypoint,
                data={"schedule_id": int(schedule.id)},
            ):
                yield

