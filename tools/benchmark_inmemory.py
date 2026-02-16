from __future__ import annotations

import asyncio, json, os, sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import typer, uvloop
from pydantic import AwareDatetime, BaseModel
from tabulate import tabulate
from tqdm.asyncio import tqdm

from pgqueuer import PgQueuer, logconfig, types
from pgqueuer.adapters.persistence.inmemory import InMemoryRepository
from pgqueuer.models import Job
from pgqueuer.qb import add_prefix


class BenchmarkResult(BaseModel):
    created_at: AwareDatetime
    elapsed: float
    throughput: float
    total_jobs: int
    avg_latency_ms: float

    def pretty_print(self):
        print(tabulate([
            ["Created At", self.created_at],
            ["Total Jobs", self.total_jobs],
            ["Elapsed Time", f"{self.elapsed:.2f}s"],
            ["Throughput", f"{self.throughput:.2f} jobs/sec"],
            ["Avg Latency", f"{self.avg_latency_ms:.2f} ms"],
        ], headers=["Field", "Value"],
        tablefmt=os.environ.get(add_prefix("TABLEFMT"), "pretty"),
        colalign=("left", "left")))


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

    async def benchmark():
        repo = InMemoryRepository()
        job_count = [0]
        start_time = datetime.now(timezone.utc)

        async def producer():
            job_id = 0
            while (datetime.now(timezone.utc) - start_time).total_seconds() < timer:
                tasks = ["task_a", "task_b", "task_c"]
                await repo.enqueue(
                    [tasks[i % 3] for i in range(batch_size)],
                    [f"{job_id + i}".encode() for i in range(batch_size)],
                    [0] * batch_size,
                )
                job_id += batch_size
                await asyncio.sleep(0)  # Yield control but don't throttle

        async def consumer():
            from pgqueuer.adapters.persistence.queries import EntrypointExecutionParameter
            from datetime import timedelta
            ep = EntrypointExecutionParameter(retry_after=timedelta(0), serialized=False, concurrency_limit=0)
            entrypoints = {"task_a": ep, "task_b": ep, "task_c": ep}
            while (datetime.now(timezone.utc) - start_time).total_seconds() < timer:
                jobs = await repo.dequeue(batch_size, entrypoints, None, None)
                job_count[0] += len(jobs)
                if not jobs:
                    await asyncio.sleep(0)  # Yield but don't throttle

        await asyncio.gather(producer(), consumer())

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
