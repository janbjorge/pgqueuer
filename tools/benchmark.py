from __future__ import annotations

import asyncio
import json
import os
import random
import signal
import sys
from abc import ABC, abstractmethod
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from itertools import count
from pathlib import Path

import typer
import uvloop
from pydantic import AwareDatetime, BaseModel
from tabulate import tabulate
from tqdm.asyncio import tqdm

from pgqueuer import PgQueuer
from pgqueuer.db import AsyncpgDriver, AsyncpgPoolDriver, PsycopgDriver, dsn
from pgqueuer.models import Job
from pgqueuer.qb import add_prefix
from pgqueuer.queries import Queries
from pgqueuer.types import QueueExecutionMode

app = typer.Typer()


class DriverEnum(str, Enum):
    apg = "apg"
    apgpool = "apgpool"
    psy = "psy"


class StrategyEnum(str, Enum):
    continuous = "continuous"
    burst = "burst"
    drain = "drain"


class BenchmarkResult(BaseModel):
    created_at: AwareDatetime
    driver: DriverEnum
    elapsed: timedelta
    github_ref_name: str
    queued: int
    rate: float
    steps: int

    def pretty_print(self) -> None:
        print(
            tabulate(
                [
                    ["Created At", self.created_at],
                    ["Driver", self.driver],
                    ["Elapsed Time", self.elapsed],
                    ["GitHub Ref Name", self.github_ref_name],
                    ["Rate", f"{self.rate:.2f}"],
                    ["Steps", self.steps],
                    ["Queued", self.queued],
                ],
                headers=["Field", "Value"],
                tablefmt=os.environ.get(add_prefix("TABLEFMT"), "pretty"),
                colalign=("left", "left"),
            )
        )


class Settings(BaseModel):
    strategy: StrategyEnum = typer.Option(
        StrategyEnum.continuous,
        help="Producer strategy: continuous, burst, or drain.",
    )
    driver: DriverEnum = typer.Option(
        DriverEnum.apg,
        help="Postgres driver to use.",
    )
    timer: timedelta = typer.Option(
        30.0,
        help="Run the benchmark for this number of seconds.",
    )
    dequeue: int = typer.Option(
        5,
        help="Number of concurrent dequeue tasks.",
    )
    dequeue_batch_size: int = typer.Option(
        10,
        help="Batch size for dequeue tasks.",
    )
    enqueue: int = typer.Option(
        1,
        help="Number of concurrent enqueue tasks.",
    )
    enqueue_batch_size: int = typer.Option(
        10,
        help="Batch size for enqueue tasks.",
    )
    requests_per_second: list[float] = typer.Option(
        [float("inf"), float("inf")],
        help="RPS for endpoints.",
    )
    concurrency_limit: list[int] = typer.Option(
        [sys.maxsize, sys.maxsize],
        help="Concurrency limit.",
    )
    drain_jobs: int = typer.Option(
        0,
        help=(
            "Prefill the queue with this many jobs and measure the time to drain. "
            "When set, producers are disabled."
        ),
    )
    burst_size: int = typer.Option(
        0,
        help="Number of jobs each producer enqueues per burst. 0 disables burst mode.",
    )
    burst_pause: float = typer.Option(
        0.0,
        help="Seconds to wait between bursts when burst_size > 0.",
    )
    output_json: Path | None = typer.Option(
        None,
        help="Output JSON file for benchmark metrics.",
    )

    def pretty_print(self) -> None:
        print(
            tabulate(
                [
                    ["Driver", self.driver],
                    ["Timer (s)", self.timer.total_seconds()],
                    ["Dequeue Tasks", self.dequeue],
                ["Dequeue Batch Size", self.dequeue_batch_size],
                ["Enqueue Tasks", self.enqueue],
                ["Enqueue Batch Size", self.enqueue_batch_size],
                ["Strategy", self.strategy],
                ["Requests Per Second", self.requests_per_second],
                ["Concurrency Limit", self.concurrency_limit],
                ["Drain Jobs", self.drain_jobs],
                ["Burst Size", self.burst_size],
                ["Burst Pause (s)", self.burst_pause],
                ["Output JSON", self.output_json or "None"],
            ],
                headers=["Field", "Value"],
                tablefmt=os.environ.get(add_prefix("TABLEFMT"), "pretty"),
                colalign=("left", "left"),
            )
        )


async def make_queries(driver: DriverEnum, conninfo: str) -> Queries:
    match driver:
        case "apg":
            import asyncpg

            return Queries(AsyncpgDriver(await asyncpg.connect(dsn=conninfo)))
        case "apgpool":
            import asyncpg

            pool = await asyncpg.create_pool(dsn=conninfo)
            assert pool is not None
            return Queries(AsyncpgPoolDriver(pool))
        case "psy":
            import psycopg

            return Queries(
                PsycopgDriver(
                    await psycopg.AsyncConnection.connect(conninfo=conninfo, autocommit=True)
                )
            )
    raise NotImplementedError(driver)


async def prefill_queue(
    queries: Queries,
    jobs: int,
    batch_size: int,
    cnt: count,
) -> None:
    """Enqueue ``jobs`` items before starting the benchmark."""

    remaining = jobs
    while remaining > 0:
        n = min(batch_size, remaining)
        await queries.enqueue(
            ["syncfetch"] * n,
            [f"{next(cnt)}".encode() for _ in range(n)],
            [0] * n,
        )
        remaining -= n


@dataclass
class Consumer:
    pgq: PgQueuer
    batch_size: int
    entrypoint_rps: list[float]
    concurrency_limits: list[int]
    bar: tqdm
    mode: QueueExecutionMode

    async def run(self) -> None:
        async_rps, sync_rps = self.entrypoint_rps
        async_cl, sync_cl = self.concurrency_limits

        @self.pgq.entrypoint(
            "asyncfetch", requests_per_second=async_rps, concurrency_limit=async_cl
        )
        async def asyncfetch(job: Job) -> None:
            self.bar.update()

        @self.pgq.entrypoint("syncfetch", requests_per_second=sync_rps, concurrency_limit=sync_cl)
        def syncfetch(job: Job) -> None:
            self.bar.update()

        await self.pgq.run(batch_size=self.batch_size, mode=self.mode)


class AbstractProducer(ABC):
    """Base class for enqueue strategies."""

    shutdown: asyncio.Event
    queries: Queries
    batch_size: int
    cnt: count

    def __init__(
        self,
        shutdown: asyncio.Event,
        queries: Queries,
        batch_size: int,
        cnt: count,
    ) -> None:
        self.shutdown = shutdown
        self.queries = queries
        self.batch_size = batch_size
        self.cnt = cnt

    @abstractmethod
    async def run(self) -> None:
        """Run the producer until ``shutdown`` is set."""


class ContinuousProducer(AbstractProducer):
    async def run(self) -> None:
        entrypoints = ["syncfetch", "asyncfetch"] * self.batch_size
        while not self.shutdown.is_set():
            await self.queries.enqueue(
                random.sample(entrypoints, k=self.batch_size),
                [f"{next(self.cnt)}".encode() for _ in range(self.batch_size)],
                [0] * self.batch_size,
            )


class BurstProducer(AbstractProducer):
    def __init__(
        self,
        shutdown: asyncio.Event,
        queries: Queries,
        batch_size: int,
        cnt: count,
        burst_size: int,
        burst_pause: float,
    ) -> None:
        super().__init__(shutdown, queries, batch_size, cnt)
        self.burst_size = burst_size
        self.burst_pause = burst_pause

    async def run(self) -> None:
        entrypoints = ["syncfetch", "asyncfetch"] * self.batch_size
        while not self.shutdown.is_set():
            jobs_in_burst = self.burst_size
            while jobs_in_burst > 0 and not self.shutdown.is_set():
                n = min(self.batch_size, jobs_in_burst)
                await self.queries.enqueue(
                    random.sample(entrypoints, k=n),
                    [f"{next(self.cnt)}".encode() for _ in range(n)],
                    [0] * n,
                )
                jobs_in_burst -= n
            if self.burst_pause:
                with suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(self.shutdown.wait(), self.burst_pause)


async def run_enqueuers(settings: Settings, shutdown: asyncio.Event) -> None:
    """Run producer tasks according to the configured strategy."""

    if settings.strategy is StrategyEnum.drain:
        return

    cnt = count()
    tasks = []
    for _ in range(settings.enqueue):
        q = await make_queries(settings.driver, dsn())
        if settings.strategy is StrategyEnum.burst:
            producer: AbstractProducer = BurstProducer(
                shutdown,
                q,
                settings.enqueue_batch_size,
                cnt,
                settings.burst_size,
                settings.burst_pause,
            )
        else:
            producer = ContinuousProducer(
                shutdown,
                q,
                settings.enqueue_batch_size,
                cnt,
            )
        tasks.append(producer.run())
    await asyncio.gather(*tasks)


async def run_dequeuers(
    settings: Settings,
    pgqs: list[PgQueuer],
    tqdm_format_dict: dict,
    mode: QueueExecutionMode,
) -> None:
    queries = [await make_queries(settings.driver, dsn()) for _ in range(settings.dequeue)]
    for q in queries:
        pgqs.append(PgQueuer(q.driver))
    with tqdm(ascii=True, unit=" job", unit_scale=True, file=sys.stdout) as bar:
        tasks = [
            Consumer(
                pgq,
                settings.dequeue_batch_size,
                settings.requests_per_second,
                settings.concurrency_limit,
                bar,
                mode,
            ).run()
            for pgq in pgqs
        ]
        await asyncio.gather(*tasks)
        tqdm_format_dict.update(bar.format_dict)


async def benchmark(settings: Settings) -> None:
    settings.pretty_print()
    await (await make_queries(settings.driver, dsn())).clear_statistics_log()
    await (await make_queries(settings.driver, dsn())).clear_queue()

    if settings.strategy is StrategyEnum.drain and settings.drain_jobs:
        preq = await make_queries(settings.driver, dsn())
        await prefill_queue(
            preq,
            settings.drain_jobs,
            settings.enqueue_batch_size,
            count(),
        )

    shutdown = asyncio.Event()
    pgqs = list[PgQueuer]()
    tqdm_format_dict = dict[str, str]()

    async def shutdown_timer() -> None:
        if settings.strategy is StrategyEnum.drain:
            await shutdown.wait()
        else:
            with suppress(TimeoutError, asyncio.TimeoutError):
                await asyncio.wait_for(shutdown.wait(), settings.timer.total_seconds())
        shutdown.set()
        for q in pgqs:
            q.shutdown.set()

    def graceful_shutdown() -> None:
        shutdown.set()
        for qm in pgqs:
            qm.shutdown.set()

    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGINT, graceful_shutdown)
    loop.add_signal_handler(signal.SIGTERM, graceful_shutdown)

    await asyncio.gather(
        run_dequeuers(
            settings,
            pgqs,
            tqdm_format_dict,
            QueueExecutionMode.drain
            if settings.strategy is StrategyEnum.drain
            else QueueExecutionMode.continuous,
        ),
        run_enqueuers(settings, shutdown)
        if settings.strategy is not StrategyEnum.drain
        else asyncio.sleep(0),
        shutdown_timer(),
    )

    qsize = await (await make_queries(settings.driver, dsn())).queue_size()
    benchmark_result = BenchmarkResult(
        created_at=datetime.now(timezone.utc),
        driver=settings.driver,
        elapsed=tqdm_format_dict["elapsed"],
        github_ref_name=os.environ.get("REF_NAME", ""),
        queued=sum(x.count for x in qsize),
        rate=float(tqdm_format_dict["n"]) / float(tqdm_format_dict["elapsed"]),
        steps=tqdm_format_dict["n"],
    )
    if settings.output_json:
        with open(settings.output_json, "w") as f:
            json.dump(benchmark_result.model_dump(mode="json"), f)
    benchmark_result.pretty_print()


@app.command()
def main(
    strategy: StrategyEnum = typer.Argument(
        StrategyEnum.continuous,
        help="Producer strategy: continuous, burst, or drain.",
    ),
    driver: DriverEnum = typer.Option(DriverEnum.apg, "-d", "--driver"),
    timer: float = typer.Option(30.0, "-t", "--time"),
    dequeue: int = typer.Option(5),
    dequeue_batch_size: int = typer.Option(10),
    enqueue: int = typer.Option(1),
    enqueue_batch_size: int = typer.Option(10),
    requests_per_second: list[float] = typer.Option([float("inf"), float("inf")]),
    concurrency_limit: list[int] = typer.Option([0, 0]),
    drain_jobs: int = typer.Option(0, "--drain"),
    burst_size: int = typer.Option(0, "--burst-size"),
    burst_pause: float = typer.Option(0.0, "--burst-pause"),
    output_json: Path | None = typer.Option(None),
) -> None:
    uvloop.run(
        benchmark(
            Settings(
                strategy=strategy,
                driver=driver,
                timer=timer,
                dequeue=dequeue,
                dequeue_batch_size=dequeue_batch_size,
                enqueue=enqueue,
                enqueue_batch_size=enqueue_batch_size,
                requests_per_second=requests_per_second,
                concurrency_limit=concurrency_limit,
                drain_jobs=drain_jobs,
                burst_size=burst_size,
                burst_pause=burst_pause,
                output_json=output_json,
            )
        )
    )


if __name__ == "__main__":
    with suppress(KeyboardInterrupt):
        app()
