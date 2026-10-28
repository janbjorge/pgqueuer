from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from pgqueuer.models import JobId, TracebackRecord


def test_from_exception() -> None:
    job_id: JobId = JobId(123)
    additional: dict[str, object] = {"detail": "Error occurred"}

    try:
        raise ValueError("Testing error")
    except Exception as exc:
        record: TracebackRecord = TracebackRecord.from_exception(exc, job_id, additional)

    assert record.job_id == job_id
    assert isinstance(record.job_id, int)
    assert record.exception_type == "ValueError"
    assert "Testing error" in record.exception_message
    assert isinstance(record.timestamp, datetime)
    assert len(record.traceback) > 0
    assert record.additional_context == additional


def test_timestamp() -> None:
    job_id: JobId = JobId(456)

    try:
        raise RuntimeError("Another test error")
    except Exception as exc:
        record: TracebackRecord = TracebackRecord.from_exception(exc, job_id)
    now: datetime = datetime.now(UTC)

    assert record.timestamp <= now
    diff: float = (now - record.timestamp).total_seconds()
    assert diff < 5  # Ensure the recorded timestamp is recent


def test_model_dump() -> None:
    job_id: JobId = JobId(789)
    additional: dict[str, object] = {"key": "value"}
    try:
        raise KeyError("Missing key")
    except Exception as exc:
        record: TracebackRecord = TracebackRecord.from_exception(exc, job_id, additional)
    record_dict: dict[str, Any] = record.model_dump()
    expected_keys: set[str] = {
        "job_id",
        "timestamp",
        "exception_type",
        "exception_message",
        "traceback",
        "additional_context",
    }
    assert set(record_dict.keys()) == expected_keys
    assert record_dict["job_id"] == job_id
    assert isinstance(record_dict["timestamp"], datetime)
    assert record_dict["additional_context"] == additional


def test_default_additional_context() -> None:
    job_id: JobId = JobId(1)
    try:
        raise Exception("Generic error")
    except Exception as exc:
        record: TracebackRecord = TracebackRecord.from_exception(exc, job_id)
    assert record.additional_context is None


def test_nul_is_spelled_out_wherever_it_appears() -> None:
    """jsonb cannot hold a NUL, so every string a record carries is free of it."""
    context: dict[str, object] = {
        "reason": "a\x00b",
        "nested": {"k\x00": ["c\x00d"]},
        "pair": ("e\x00f", 2),
        "n": 1,
    }
    try:
        raise ValueError("x\x00y")
    except Exception as exc:
        record = TracebackRecord.from_exception(exc, JobId(1), context)

    assert record.exception_message == "x\\x00y"
    assert "\x00" not in record.traceback
    assert "x\\x00y" in record.traceback
    assert record.additional_context == {
        "reason": "a\\x00b",
        "nested": {"k\\x00": ["c\\x00d"]},
        "pair": ("e\\x00f", 2),
        "n": 1,
    }
    assert "\\u0000" not in record.model_dump_json()
