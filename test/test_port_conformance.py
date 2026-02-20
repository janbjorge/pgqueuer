"""Verify that Queries structurally satisfies all port protocols."""

from __future__ import annotations

from pgqueuer.adapters.inmemory import InMemoryQueries
from pgqueuer.ports import (
    NotificationPort,
    QueueRepositoryPort,
    ScheduleRepositoryPort,
    SchemaManagementPort,
    TracingProtocol,
)
from pgqueuer.queries import Queries
from pgqueuer.tracing import LogfireTracing, SentryTracing


def test_queries_has_queue_repository_methods() -> None:
    required = {
        "dequeue",
        "enqueue",
        "log_jobs",
        "clear_queue",
        "queue_size",
        "mark_job_as_cancelled",
        "update_heartbeat",
        "queued_work",
        "queue_log",
        "job_status",
    }
    assert required <= set(dir(Queries))


def test_queries_has_schedule_repository_methods() -> None:
    required = {
        "insert_schedule",
        "fetch_schedule",
        "set_schedule_queued",
        "update_schedule_heartbeat",
        "peak_schedule",
        "delete_schedule",
        "clear_schedule",
    }
    assert required <= set(dir(Queries))


def test_queries_has_notification_methods() -> None:
    required = {"notify_entrypoint_rps", "notify_job_cancellation", "notify_health_check"}
    assert required <= set(dir(Queries))


def test_queries_has_schema_management_methods() -> None:
    required = {
        "install",
        "uninstall",
        "upgrade",
        "has_table",
        "table_has_column",
        "table_has_index",
        "has_user_defined_enum",
    }
    assert required <= set(dir(Queries))


def test_inmemory_has_queue_repository_methods() -> None:
    required = {
        "dequeue",
        "enqueue",
        "log_jobs",
        "clear_queue",
        "queue_size",
        "mark_job_as_cancelled",
        "update_heartbeat",
        "queued_work",
        "queue_log",
        "job_status",
    }
    assert required <= set(dir(InMemoryQueries))


def test_inmemory_has_schedule_repository_methods() -> None:
    required = {
        "insert_schedule",
        "fetch_schedule",
        "set_schedule_queued",
        "update_schedule_heartbeat",
        "peak_schedule",
        "delete_schedule",
        "clear_schedule",
    }
    assert required <= set(dir(InMemoryQueries))


def test_inmemory_has_notification_methods() -> None:
    required = {"notify_entrypoint_rps", "notify_job_cancellation", "notify_health_check"}
    assert required <= set(dir(InMemoryQueries))


def test_inmemory_has_schema_management_methods() -> None:
    required = {
        "install",
        "uninstall",
        "upgrade",
        "has_table",
        "table_has_column",
        "table_has_index",
        "has_user_defined_enum",
    }
    assert required <= set(dir(InMemoryQueries))


def test_tracing_implementations_have_required_methods() -> None:
    required = {"trace_publish", "trace_process"}
    assert required <= set(dir(LogfireTracing))
    assert required <= set(dir(SentryTracing))
