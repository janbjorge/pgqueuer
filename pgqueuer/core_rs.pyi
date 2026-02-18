"""Type stubs for pgqueuer.core_rs (Rust PyO3 extension)."""

from __future__ import annotations

class InMemoryCore:
    def __init__(self) -> None: ...
    def enqueue_batch(
        self,
        entrypoints: list[str],
        payloads: list[bytes | None],
        priorities: list[int],
        execute_after_us: list[int],
        dedupe_keys: list[str | None],
        headers: list[str | None],
        now_us: int,
    ) -> list[int]: ...
    def dequeue_batch(
        self,
        batch_size: int,
        ep_names: list[str],
        ep_retry_after_us: list[int],
        ep_serialized: list[bool],
        ep_concurrency_limits: list[int],
        queue_manager_id_bytes: list[int],
        global_concurrency_limit: int | None,
        now_us: int,
    ) -> list[tuple[int, int, int, int, int, int, str, bytes | None, bytes | None, str | None]]: ...
    def log_jobs(
        self,
        job_ids: list[int],
        statuses: list[str],
        tracebacks: list[str | None],
        now_us: int,
    ) -> None: ...
    def update_heartbeat(self, job_ids: list[int], now_us: int) -> None: ...
    def mark_cancelled(self, job_ids: list[int], now_us: int) -> None: ...
    def clear_queue(self, entrypoints: list[str] | None, now_us: int) -> None: ...
    def queue_size(self) -> list[tuple[str, int, str, int]]: ...
    def queued_work(self, entrypoints: list[str]) -> int: ...
    def queue_log(self) -> list[tuple[int, int, str, int, str, str | None, bool]]: ...
    def job_status(self, job_ids: list[int]) -> list[tuple[int, str]]: ...
    def log_statistics(
        self,
        tail: int | None,
        since_us: int | None,
    ) -> list[tuple[str, int, str, int, int]]: ...
