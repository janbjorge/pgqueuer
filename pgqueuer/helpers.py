"""Backward-compatibility shim. Canonical: pgqueuer.core.helpers"""
from pgqueuer.core.helpers import (
    ExponentialBackoff,
    add_schema_to_dsn,
    merge_tracing_headers,
    normalize_cron_expression,
    retry_timer_buffer_timeout,
    timeout_with_jitter,
    timer,
    utc_now,
    wait_for_notice_event,
)

__all__ = [
    "ExponentialBackoff",
    "add_schema_to_dsn",
    "merge_tracing_headers",
    "normalize_cron_expression",
    "retry_timer_buffer_timeout",
    "timeout_with_jitter",
    "timer",
    "utc_now",
    "wait_for_notice_event",
]
