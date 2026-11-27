from __future__ import annotations

import pytest

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


# asyncpg returns jsonb as text. psycopg returns it as parsed data.
@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        pytest.param("[1]", None, id="list-text"),
        pytest.param([1], None, id="list-parsed"),
        pytest.param('"x"', None, id="string-text"),
        pytest.param("x", None, id="string-parsed"),
        pytest.param("1", None, id="number-text"),
        pytest.param(1, None, id="number-parsed"),
        pytest.param("true", None, id="bool-text"),
        pytest.param(True, None, id="bool-parsed"),
        pytest.param("null", None, id="json-null-text"),
        pytest.param(None, None, id="null"),
        pytest.param(
            '{"otel": {"traceparent": "00-abc"}}',
            {"otel": {"traceparent": "00-abc"}},
            id="object-text",
        ),
        pytest.param(
            {"otel": {"traceparent": "00-abc"}},
            {"otel": {"traceparent": "00-abc"}},
            id="object-parsed",
        ),
    ],
)
def test_job_headers_keep_only_a_json_object(
    raw: object,
    expected: dict[str, object] | None,
) -> None:
    """Job headers keep a JSON object. Every other JSON value becomes None."""
    job = Job.model_validate({**mocked_job().model_dump(), "headers": raw})
    assert job.headers == expected
    assert job.otel_headers() == (None if expected is None else expected["otel"])


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
