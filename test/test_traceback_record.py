from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pgqueuer.models import JobId, TracebackRecord


def test_from_exception() -> None:
    # Arrange
    job_id: JobId = JobId(123)
    additional: dict[str, str] = {"detail": "Error occurred"}
    # Act
    try:
        raise ValueError("Testing error")
    except Exception as exc:
        record: TracebackRecord = TracebackRecord.from_exception(exc, job_id, additional)
    # Assert
    assert record.job_id == job_id
    assert isinstance(record.job_id, int)
    assert record.exception_type == "ValueError"
    assert "Testing error" in record.exception_message
    assert isinstance(record.timestamp, datetime)
    assert len(record.traceback) > 0
    assert record.additional_context == additional


def test_timestamp() -> None:
    # Arrange
    job_id: JobId = JobId(456)
    # Act
    try:
        raise RuntimeError("Another test error")
    except Exception as exc:
        record: TracebackRecord = TracebackRecord.from_exception(exc, job_id)
    now: datetime = datetime.now(timezone.utc)
    # Assert
    assert record.timestamp <= now
    diff: float = (now - record.timestamp).total_seconds()
    assert diff < 5  # Ensure the recorded timestamp is recent


def test_model_dump() -> None:
    # Arrange
    job_id: JobId = JobId(789)
    additional: dict[str, str] = {"key": "value"}
    # Act
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
    # Assert
    assert set(record_dict.keys()) == expected_keys
    assert record_dict["job_id"] == job_id
    assert isinstance(record_dict["timestamp"], datetime)
    assert record_dict["additional_context"] == additional


def test_default_additional_context() -> None:
    # Arrange
    job_id: JobId = JobId(1)
    # Act
    try:
        raise Exception("Generic error")
    except Exception as exc:
        record: TracebackRecord = TracebackRecord.from_exception(exc, job_id)
    # Assert
    assert record.additional_context == {}
