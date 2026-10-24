from __future__ import annotations

from pgqueuer.domain.models import Job, Log, TracebackRecord
from pgqueuer.domain.types import JobId
from test.helpers import mocked_job


def test_job_headers_accept_parsed_dict_and_json() -> None:
    """Headers validate from an already-parsed dict as well as from JSON text."""
    headers: dict[str, object] = {"otel": {"traceparent": "00-abc"}}
    from_dict = mocked_job(headers=headers)
    from_json = Job.model_validate(
        {**from_dict.model_dump(), "headers": b'{"otel": {"traceparent": "00-abc"}}'}
    )
    assert from_dict.headers == from_json.headers == headers


def test_log_traceback_accepts_parsed_dict_and_json() -> None:
    """A log traceback validates from a parsed dict (psycopg) and from JSON text (asyncpg)."""
    record = TracebackRecord.from_exception(ValueError("boom"), job_id=JobId(1))
    row: dict[str, object] = {
        "created": "2024-01-01T00:00:00Z",
        "job_id": 1,
        "status": "exception",
        "priority": 0,
        "entrypoint": "ep",
        "aggregated": False,
    }
    from_dict = Log.model_validate({**row, "traceback": record.model_dump(mode="json")})
    from_json = Log.model_validate({**row, "traceback": record.model_dump_json()})
    assert from_dict.traceback == from_json.traceback == record
    assert Log.model_validate({**row, "traceback": None}).traceback is None


def test_header_section_returns_only_nested_dicts() -> None:
    job = mocked_job(headers={"otel": {"traceparent": "00-abc"}, "logfire": "not-a-dict"})
    assert job.otel_headers() == {"traceparent": "00-abc"}
    assert job.logfire_headers() is None
    assert job.sentry_headers() is None
    assert mocked_job(headers=None).otel_headers() is None
