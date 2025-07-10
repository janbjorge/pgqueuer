from __future__ import annotations

import contextlib
from dataclasses import dataclass, field
from typing import Any, Iterator

from . import models


@dataclass
class Telemetry:
    """Lazy Sentry instrumentation for queue and schedule execution."""

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

    @contextlib.contextmanager
    def job_span(self, job: models.Job) -> Iterator[None]:
        """Context manager tracing the execution of ``job``."""
        if not self._load_sdk():
            yield
        else:
            assert self._sdk
            with self._sdk.start_span(
                op="pgqueuer.job",
                description=job.entrypoint,
                data={"job_id": int(job.id)},
            ):
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

