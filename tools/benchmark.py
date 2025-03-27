from __future__ import annotations

import asyncio
import json
import os
import random
import signal
import sys
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

app = typer.Typer()


class DriverEnum(str, Enum):
    apg = "apg"
    apgpool = "apgpool"
    psy = "psy"


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
                    ["Requests Per Second", self.requests_per_second],
                    ["Concurrency Limit", self.concurrency_limit],
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
    entrypoint_rps: list[float]
    concurrency_limits: list[int]
    bar: tqdm

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

        await self.pgq.run(batch_size=self.batch_size)


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


async def run_enqueuers(settings: Settings, shutdown: asyncio.Event) -> None:
    cnt = count()
    tasks = []
    for _ in range(settings.enqueue):
        q = await make_queries(settings.driver, dsn())
        p = Producer(shutdown, q, settings.enqueue_batch_size, cnt)
        tasks.append(p.run())
    await asyncio.gather(*tasks)


async def run_dequeuers(
    settings: Settings,
    pgqs: list[PgQueuer],
    tqdm_format_dict: dict,
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
            ).run()
            for pgq in pgqs
        ]
        await asyncio.gather(*tasks)
        tqdm_format_dict.update(bar.format_dict)


async def benchmark(settings: Settings) -> None:
    settings.pretty_print()
    await (await make_queries(settings.driver, dsn())).clear_statistics_log()
    await (await make_queries(settings.driver, dsn())).clear_queue()

    shutdown = asyncio.Event()
    pgqs = list[PgQueuer]()
    tqdm_format_dict = dict[str, str]()

    async def shutdown_timer() -> None:
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
        run_dequeuers(settings, pgqs, tqdm_format_dict),
        run_enqueuers(settings, shutdown),
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
    driver: DriverEnum = typer.Option(DriverEnum.apg, "-d", "--driver"),
    timer: float = typer.Option(30.0, "-t", "--time"),
    dequeue: int = typer.Option(5),
    dequeue_batch_size: int = typer.Option(10),
    enqueue: int = typer.Option(1),
    enqueue_batch_size: int = typer.Option(10),
    requests_per_second: list[float] = typer.Option([float("inf"), float("inf")]),
    concurrency_limit: list[int] = typer.Option([0, 0]),
    output_json: Path | None = typer.Option(None),
) -> None:
    uvloop.run(
        benchmark(
            Settings(
                driver=driver,
                timer=timer,
                dequeue=dequeue,
                dequeue_batch_size=dequeue_batch_size,
                enqueue=enqueue,
                enqueue_batch_size=enqueue_batch_size,
                requests_per_second=requests_per_second,
                concurrency_limit=concurrency_limit,
                output_json=output_json,
            )
        )
    )


if __name__ == "__main__":
    with suppress(KeyboardInterrupt):
        app()
