from types import SimpleNamespace
import types

import pytest

from pgqueuer import models, telemetry, helpers


def test_job_span_without_sdk(monkeypatch: pytest.MonkeyPatch) -> None:
    tel = telemetry.Telemetry()
    # ensure sentry_sdk import fails
    monkeypatch.setitem(__import__('sys').modules, 'sentry_sdk', None)
    job = models.Job(
        id=models.JobId(1),
        priority=1,
        created=helpers.utc_now(),
        updated=helpers.utc_now(),
        heartbeat=helpers.utc_now(),
        execute_after=helpers.utc_now(),
        status="queued",
        entrypoint="ep",
        payload=None,
        queue_manager_id=None,
        headers=None,
    )
    with tel.job_span(job):
        pass


def test_job_span_with_sdk(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, bool] = {}

    class DummySpan:
        def __enter__(self) -> None:
            captured['entered'] = True

        def __exit__(
            self,
            exc_type: type[BaseException] | None,
            exc: BaseException | None,
            tb: types.TracebackType | None,
        ) -> None:
            captured['exited'] = True

    dummy_sdk = SimpleNamespace(start_span=lambda **_: DummySpan())
    monkeypatch.setitem(__import__('sys').modules, 'sentry_sdk', dummy_sdk)
    tel = telemetry.Telemetry()
    job = models.Job(
        id=models.JobId(1),
        priority=1,
        created=helpers.utc_now(),
        updated=helpers.utc_now(),
        heartbeat=helpers.utc_now(),
        execute_after=helpers.utc_now(),
        status="queued",
        entrypoint="ep",
        payload=None,
        queue_manager_id=None,
        headers=None,
    )
    with tel.job_span(job):
        assert captured.get('entered')
    assert captured.get('exited')




