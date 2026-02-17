from __future__ import annotations

import asyncio
import json
import os
import signal
import sys
from datetime import datetime, timezone
from pathlib import Path

import typer
import uvloop
from pydantic import AwareDatetime, BaseModel
from tabulate import tabulate
from tqdm.asyncio import tqdm

from pgqueuer import logconfig
from pgqueuer.adapters.persistence.inmemory import InMemoryRepository
from pgqueuer.qb import add_prefix


class BenchmarkResult(BaseModel):
    created_at: AwareDatetime
    elapsed: float
    throughput: float
    total_jobs: int
    avg_latency_ms: float

    def pretty_print(self):
        print(
            tabulate(
                [
                    ["Created At", self.created_at],
                    ["Total Jobs", self.total_jobs],
                    ["Elapsed Time", f"{self.elapsed:.2f}s"],
                    ["Throughput", f"{self.throughput:.2f} jobs/sec"],
                    ["Avg Latency", f"{self.avg_latency_ms:.2f} ms"],
                ],
                headers=["Field", "Value"],
                tablefmt=os.environ.get(add_prefix("TABLEFMT"), "pretty"),
                colalign=("left", "left"),
            )
        )


def job_progress_bar(total=None):
    return tqdm(total=total, ascii=True, unit=" job", unit_scale=True, file=sys.stdout)


app = typer.Typer(add_completion=False)


@app.command()
def main(
    timer: float = typer.Option(10.0, "-t", "--time"),
    batch_size: int = typer.Option(10),
    output_json: Path | None = typer.Option(None),
):
    """In-memory benchmark: single PgQueuer with producer/consumer."""

    hard_timeout = timer * 1.1  # 10% grace period

    # Hard process-level timeout as a safety net
    def _hard_timeout_handler(signum: int, frame: object) -> None:
        print(f"\nHard timeout ({hard_timeout:.1f}s) reached — aborting.", file=sys.stderr)
        sys.exit(1)

    signal.signal(signal.SIGALRM, _hard_timeout_handler)
    signal.alarm(int(hard_timeout) + 1)

    async def benchmark():
        from datetime import timedelta
        from time import monotonic

        from pgqueuer.adapters.persistence.queries import EntrypointExecutionParameter

        repo = InMemoryRepository()
        job_count = [0]
        stop = False
        start_time = datetime.now(timezone.utc)
        mono_start = monotonic()
        deadline = mono_start + timer

        async def producer():
            nonlocal stop
            job_id = 0
            tasks = ["task_a", "task_b", "task_c"]
            entrypoints = [tasks[i % len(tasks)] for i in range(batch_size)]
            payloads: list[bytes | None] = [None] * batch_size
            priorities = [0] * batch_size
            i = 0
            while not stop:
                await repo.enqueue(entrypoints, payloads, priorities)
                job_id += batch_size
                i += 1
                # Yield to event loop periodically and check deadline
                if i % 64 == 0:
                    await asyncio.sleep(0)
                    if monotonic() >= deadline:
                        stop = True

        async def consumer():
            nonlocal stop
            ep = EntrypointExecutionParameter(
                retry_after=timedelta(0), serialized=False, concurrency_limit=0
            )
            entrypoints = {"task_a": ep, "task_b": ep, "task_c": ep}
            i = 0
            while not stop:
                jobs = await repo.dequeue(batch_size, entrypoints, None, None)
                if jobs:
                    job_count[0] += len(jobs)
                i += 1
                if i % 64 == 0:
                    await asyncio.sleep(0)

        await asyncio.wait_for(
            asyncio.gather(producer(), consumer()),
            timeout=hard_timeout,
        )

        elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
        return BenchmarkResult(
            created_at=datetime.now(timezone.utc),
            elapsed=elapsed,
            throughput=job_count[0] / elapsed if elapsed > 0 else 0,
            total_jobs=job_count[0],
            avg_latency_ms=(elapsed * 1000) / job_count[0] if job_count[0] > 0 else 0,
        )

    print()
    print("🚀 PgQueuer In-Memory Benchmark")
    print()
    print(f"Duration: {timer}s, Batch: {batch_size}")
    print()

    result = uvloop.run(benchmark())
    result.pretty_print()

    if output_json:
        with open(output_json, "w") as f:
            json.dump(result.model_dump(mode="json"), f)
        print(f"\nSaved to {output_json}")


if __name__ == "__main__":
    logconfig.setup_fancy_logger(logconfig.LogLevel.INFO)
    app()
