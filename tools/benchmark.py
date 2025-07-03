from __future__ import annotations

import asyncio
import json
import os
import random
import signal
import sys
from contextlib import suppress
from dataclasses import dataclass, field as dataclass_field
from datetime import datetime, timedelta, timezone
from enum import Enum
from itertools import count
from pathlib import Path
from typing import Protocol

import typer
import uvloop
from pydantic import AwareDatetime, BaseModel
from tabulate import tabulate
from tqdm.asyncio import tqdm

from pgqueuer import PgQueuer, types
from pgqueuer.db import AsyncpgDriver, AsyncpgPoolDriver, PsycopgDriver, dsn
from pgqueuer.models import Job
from pgqueuer.qb import add_prefix
from pgqueuer.queries import Queries


def job_progress_bar(total: int | None = None) -> tqdm:
    """Return a progress bar configured for job throughput measurements."""
    return tqdm(total=total, ascii=True, unit=" job", unit_scale=True, file=sys.stdout)


app = typer.Typer(add_completion=False)


class DriverEnum(str, Enum):
    apg = "apg"
    apgpool = "apgpool"
    psy = "psy"


class StrategyEnum(str, Enum):
    """Available benchmarking strategies."""

    throughput = "throughput"
    drain = "drain"


class BenchmarkResult(BaseModel):
    created_at: AwareDatetime
    driver: DriverEnum
    elapsed: timedelta
    strategy: StrategyEnum
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


class ThroughputSettings(BaseModel):
    driver: DriverEnum = typer.Option(
        DriverEnum.apg,
        help="Postgres driver to use.",
    )
    strategy: StrategyEnum = typer.Option(
        StrategyEnum.throughput,
        help="Benchmarking strategy to execute.",
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
    output_json: Path | None = typer.Option(
        None,
        help="Output JSON file for benchmark metrics.",
    )

    def pretty_print(self) -> None:
        print(
            tabulate(
                [
                    ["Driver", self.driver],
                    ["Strategy", self.strategy],
                    ["Timer (s)", self.timer.total_seconds()],
                    ["Dequeue Tasks", self.dequeue],
                    ["Dequeue Batch Size", self.dequeue_batch_size],
                    ["Enqueue Tasks", self.enqueue],
                    ["Enqueue Batch Size", self.enqueue_batch_size],
                    ["Output JSON", self.output_json or "None"],
                ],
                headers=["Field", "Value"],
                tablefmt=os.environ.get(add_prefix("TABLEFMT"), "pretty"),
                colalign=("left", "left"),
            )
        )


class DrainSettings(BaseModel):
    driver: DriverEnum = typer.Option(
        DriverEnum.apg,
        help="Postgres driver to use.",
    )
    strategy: StrategyEnum = typer.Option(
        StrategyEnum.drain,
        help="Benchmarking strategy to execute.",
    )
    jobs: int = typer.Option(
        50_000,
        help="Number of jobs to enqueue for the drain strategy.",
    )
    dequeue: int = typer.Option(
        5,
        help="Number of concurrent dequeue tasks.",
    )
    dequeue_batch_size: int = typer.Option(
        10,
        help="Batch size for dequeue tasks.",
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
                    ["Strategy", self.strategy],
                    ["Jobs", self.jobs],
                    ["Dequeue Tasks", self.dequeue],
                    ["Dequeue Batch Size", self.dequeue_batch_size],
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


@dataclass
class Consumer:
    pgq: PgQueuer
    batch_size: int
    bar: tqdm
    mode: types.QueueExecutionMode = types.QueueExecutionMode.continuous

    async def run(self) -> None:
        @self.pgq.entrypoint("asyncfetch")
        async def asyncfetch(job: Job) -> None:
            self.bar.update()

        @self.pgq.entrypoint("syncfetch")
        def syncfetch(job: Job) -> None:
            self.bar.update()

        await self.pgq.run(batch_size=self.batch_size, mode=self.mode)


@dataclass
class Producer:
    shutdown: asyncio.Event
    queries: Queries
    batch_size: int
    cnt: count

    async def run(self) -> None:
        entrypoints = ["syncfetch", "asyncfetch"] * self.batch_size
        while not self.shutdown.is_set():
            await self.queries.enqueue(
                random.sample(entrypoints, k=self.batch_size),
                [f"{next(self.cnt)}".encode() for _ in range(self.batch_size)],
                [0] * self.batch_size,
            )


class BenchmarkStrategy(Protocol):
    """Strategy interface for running benchmarks."""

    async def setup(self) -> None:
        """Prepare environment for benchmarking."""

    async def run(self) -> BenchmarkResult:
        """Execute the benchmark and return results."""

    async def teardown(self) -> None:
        """Clean up resources after the benchmark."""


@dataclass
class ThroughputStrategy:
    """Replicate the original benchmark measuring throughput."""

    settings: ThroughputSettings
    shutdown: asyncio.Event = dataclass_field(
        default_factory=asyncio.Event,
        init=False,
    )
    pgqs: list[PgQueuer] = dataclass_field(
        default_factory=list,
        init=False,
    )
    tqdm_format_dict: dict[str, str] = dataclass_field(
        default_factory=dict,
        init=False,
    )

    async def run_enqueuers(self) -> None:
        cnt = count()
        tasks = []
        for _ in range(self.settings.enqueue):
            q = await make_queries(self.settings.driver, dsn())
            p = Producer(self.shutdown, q, self.settings.enqueue_batch_size, cnt)
            tasks.append(p.run())
        await asyncio.gather(*tasks)

    async def run_dequeuers(self) -> None:
        queries = [
            await make_queries(self.settings.driver, dsn()) for _ in range(self.settings.dequeue)
        ]
        for q in queries:
            self.pgqs.append(PgQueuer(q.driver))
        with job_progress_bar() as bar:
            tasks = [
                Consumer(pgq, self.settings.dequeue_batch_size, bar).run() for pgq in self.pgqs
            ]
            await asyncio.gather(*tasks)
            self.tqdm_format_dict.update(bar.format_dict)

    async def shutdown_timer(self) -> None:
        with suppress(TimeoutError, asyncio.TimeoutError):
            await asyncio.wait_for(
                self.shutdown.wait(),
                self.settings.timer.total_seconds(),
            )

        self.graceful_shutdown()

    async def setup(self) -> None:
        loop = asyncio.get_event_loop()
        loop.add_signal_handler(signal.SIGINT, self.graceful_shutdown)
        loop.add_signal_handler(signal.SIGTERM, self.graceful_shutdown)

        self.settings.pretty_print()
        queries = await make_queries(self.settings.driver, dsn())
        await queries.clear_statistics_log()
        await queries.clear_queue()

    def graceful_shutdown(self) -> None:
        self.shutdown.set()
        for qm in self.pgqs:
            qm.shutdown.set()

    async def run(self) -> BenchmarkResult:
        await asyncio.gather(
            self.run_dequeuers(),
            self.run_enqueuers(),
            self.shutdown_timer(),
        )

        qsize = await (await make_queries(self.settings.driver, dsn())).queue_size()
        return BenchmarkResult(
            created_at=datetime.now(timezone.utc),
            driver=self.settings.driver,
            strategy=StrategyEnum.throughput,
            elapsed=self.tqdm_format_dict["elapsed"],
            github_ref_name=os.environ.get("REF_NAME", ""),
            queued=sum(x.count for x in qsize),
            rate=float(self.tqdm_format_dict["n"]) / float(self.tqdm_format_dict["elapsed"]),
            steps=self.tqdm_format_dict["n"],
        )

    async def teardown(self) -> None:  # pragma: no cover - nothing to clean up
        pass


@dataclass
class DrainStrategy:
    """Benchmark strategy that drains a pre-populated queue."""

    settings: DrainSettings
    pgqs: list[PgQueuer] = dataclass_field(default_factory=list, init=False)
    tqdm_format_dict: dict[str, str] = dataclass_field(default_factory=dict, init=False)

    async def setup(self) -> None:
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self.graceful_shutdown)

        self.settings.pretty_print()
        queries = await make_queries(self.settings.driver, dsn())
        await queries.clear_statistics_log()
        await queries.clear_queue()

        entrypoints = ["syncfetch", "asyncfetch"] * self.settings.jobs
        await queries.enqueue(
            random.sample(entrypoints, k=self.settings.jobs),
            [b"" for _ in range(self.settings.jobs)],
            [0] * self.settings.jobs,
        )

    def graceful_shutdown(self) -> None:
        for qm in self.pgqs:
            qm.shutdown.set()

    async def run(self) -> BenchmarkResult:
        queries = [
            await make_queries(self.settings.driver, dsn()) for _ in range(self.settings.dequeue)
        ]
        for q in queries:
            self.pgqs.append(PgQueuer(q.driver))

        start = datetime.now(timezone.utc)
        with job_progress_bar(total=self.settings.jobs) as bar:
            tasks = [
                Consumer(
                    pgq,
                    self.settings.dequeue_batch_size,
                    bar,
                    types.QueueExecutionMode.drain,
                ).run()
                for pgq in self.pgqs
            ]
            await asyncio.gather(*tasks)
            self.tqdm_format_dict.update(bar.format_dict)
        elapsed = datetime.now(timezone.utc) - start
        return BenchmarkResult(
            created_at=datetime.now(timezone.utc),
            driver=self.settings.driver,
            strategy=StrategyEnum.drain,
            elapsed=elapsed,
            github_ref_name=os.environ.get("REF_NAME", ""),
            queued=0,
            rate=self.settings.jobs / elapsed.total_seconds(),
            steps=self.settings.jobs,
        )

    async def teardown(self) -> None:  # pragma: no cover - nothing to clean up
        pass


@dataclass
class BenchmarkRunner:
    """Execute benchmarks using a chosen strategy."""

    strategy: BenchmarkStrategy
    output_json: Path | None

    async def execute(self) -> None:
        await self.strategy.setup()
        result = await self.strategy.run()
        await self.strategy.teardown()
        if self.output_json:
            with open(self.output_json, "w") as f:
                json.dump(result.model_dump(mode="json"), f)
        result.pretty_print()


@app.command()
def main(
    driver: DriverEnum = typer.Option(DriverEnum.apg, "-d", "--driver"),
    timer: float = typer.Option(30.0, "-t", "--time"),
    dequeue: int = typer.Option(5),
    dequeue_batch_size: int = typer.Option(10),
    enqueue: int = typer.Option(1),
    enqueue_batch_size: int = typer.Option(10),
    jobs: int = typer.Option(50_000, help="Number of jobs for the drain strategy"),
    output_json: Path | None = typer.Option(None),
    strategy: StrategyEnum = typer.Option(StrategyEnum.throughput, "-s", "--strategy"),
) -> None:
    if strategy is StrategyEnum.drain:
        drain_settings = DrainSettings(
            driver=driver,
            strategy=strategy,
            jobs=jobs,
            dequeue=dequeue,
            dequeue_batch_size=dequeue_batch_size,
            output_json=output_json,
        )
        strategy_impl: BenchmarkStrategy = DrainStrategy(drain_settings)
    else:
        tp_settings = ThroughputSettings(
            driver=driver,
            strategy=strategy,
            timer=timer,
            dequeue=dequeue,
            dequeue_batch_size=dequeue_batch_size,
            enqueue=enqueue,
            enqueue_batch_size=enqueue_batch_size,
            output_json=output_json,
        )
        strategy_impl = ThroughputStrategy(tp_settings)
    runner = BenchmarkRunner(strategy=strategy_impl, output_json=output_json)
    uvloop.run(runner.execute())


if __name__ == "__main__":
    with suppress(KeyboardInterrupt):
        app()
