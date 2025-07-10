import types
from types import SimpleNamespace

import pytest

from pgqueuer import helpers, models, telemetry


def test_job_span_without_sdk(monkeypatch: pytest.MonkeyPatch) -> None:
    tel = telemetry.Telemetry()
    monkeypatch.setitem(__import__("sys").modules, "sentry_sdk", None)
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
        def __enter__(self) -> "DummySpan":
            captured["entered"] = True
            return self

        def set_data(self, *_: object, **__: object) -> None:
            pass

        def __exit__(
            self,
            exc_type: type[BaseException] | None,
            exc: BaseException | None,
            tb: types.TracebackType | None,
        ) -> None:
            captured["exited"] = True

    dummy_sdk = SimpleNamespace(
        start_span=lambda **_: DummySpan(),
        get_traceparent=lambda: "tp",
        get_baggage=lambda: "bg",
    )
    monkeypatch.setitem(__import__("sys").modules, "sentry_sdk", dummy_sdk)
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
        assert captured.get("entered")
    assert captured.get("exited")


def test_trace_headers(monkeypatch: pytest.MonkeyPatch) -> None:
    dummy_sdk = SimpleNamespace(get_traceparent=lambda: "tp", get_baggage=lambda: "bg")
    monkeypatch.setitem(__import__("sys").modules, "sentry_sdk", dummy_sdk)
    tel = telemetry.Telemetry()
    assert tel.trace_headers() == {"sentry-trace": "tp", "baggage": "bg"}




