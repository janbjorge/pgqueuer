from __future__ import annotations

from pgqueuer.domain.models import Job
from test.helpers import mocked_job


def test_job_headers_accept_parsed_dict_and_json() -> None:
    """Headers validate from an already-parsed dict as well as from JSON text."""
    headers: dict[str, object] = {"otel": {"traceparent": "00-abc"}}
    from_dict = mocked_job(headers=headers)
    from_json = Job.model_validate(
        {**from_dict.model_dump(), "headers": b'{"otel": {"traceparent": "00-abc"}}'}
    )
    assert from_dict.headers == from_json.headers == headers


def test_header_section_returns_only_nested_dicts() -> None:
    job = mocked_job(headers={"otel": {"traceparent": "00-abc"}, "logfire": "not-a-dict"})
    assert job.otel_headers() == {"traceparent": "00-abc"}
    assert job.logfire_headers() is None
    assert job.sentry_headers() is None
    assert mocked_job(headers=None).otel_headers() is None
