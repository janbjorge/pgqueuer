"""Ramp the queue and EXPLAIN ANALYZE the prepared dequeue statement at each depth."""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import json
import uuid
from datetime import timedelta
from pathlib import Path
from typing import Literal, get_args

import asyncpg
import typer
from tabulate import tabulate

from pgqueuer.adapters.persistence.composer import ComposedQuery
from pgqueuer.adapters.persistence.qb import QueryQueueBuilder
from pgqueuer.adapters.persistence.query_helpers import cell
from pgqueuer.domain.settings import DBSettings
from pgqueuer.domain.types import QueueEntrypoint, QueueManagerId

PlanMode = Literal["auto", "force_custom_plan", "force_generic_plan"]
# PostgreSQL weighs the generic plan against custom ones only after five executions.
WARMUP_EXECUTIONS = 6
EXPLAIN_RUNS = 5
STATEMENT = "pgq_dequeue_plan"

app = typer.Typer(add_completion=False)


@dataclasses.dataclass(frozen=True)
class PlanRecord:
    concurrency_limit: int
    rows: int
    mode: PlanMode
    chosen_plan: str | None
    total_cost: float
    jit: bool
    jit_ms: float | None
    planning_ms: float
    execution_ms: float
    claimed_rows: int


def number(mapping: dict[str, object], key: str) -> float:
    value = mapping[key]
    if not isinstance(value, int | float):
        raise TypeError(f"{key!r}: expected a number, got {type(value).__name__}")
    return float(value)


def parse_plan(raw: object) -> dict[str, object]:
    document: object = json.loads(raw) if isinstance(raw, str) else raw
    if not (isinstance(document, list) and isinstance(document[0], dict)):
        raise TypeError("unexpected EXPLAIN (FORMAT JSON) output")
    return document[0]


async def rolled_back(conn: asyncpg.Connection, sql: str) -> object:
    """Run *sql* in a transaction that is always rolled back, since dequeue writes."""
    async with contextlib.AsyncExitStack() as stack:
        transaction = conn.transaction()
        await transaction.start()
        stack.push_async_callback(transaction.rollback)
        return await conn.fetchval(sql)


async def plan_counts(conn: asyncpg.Connection) -> dict[str, int]:
    row = await conn.fetchrow(
        "SELECT generic_plans, custom_plans FROM pg_prepared_statements WHERE name = $1",
        STATEMENT,
    )
    return {key: cell(dict(row or {}), key, int) for key in ("generic_plans", "custom_plans")}


async def execute_call(conn: asyncpg.Connection, args: tuple[object, ...]) -> str:
    """Spell EXECUTE with literal arguments; EXPLAIN EXECUTE cannot take binds."""
    types = await conn.fetch(
        """SELECT parameter_type::text AS parameter_type FROM pg_prepared_statements,
            unnest(parameter_types) WITH ORDINALITY AS parameter(parameter_type, position)
        WHERE name = $1 ORDER BY position""",
        STATEMENT,
    )
    literals: list[str] = []
    for arg, row in zip(args, types, strict=True):
        type_name = cell(dict(row), "parameter_type", str)
        literal = await conn.fetchval(f"SELECT quote_literal($1::{type_name})", arg)
        literals.append(f"{literal}::{type_name}")
    return f"EXECUTE {STATEMENT}({', '.join(literals)})"


async def explain_mode(
    conn: asyncpg.Connection, limit: int, rows: int, mode: PlanMode, query: ComposedQuery
) -> PlanRecord:
    sql, args = query.sql, query.args
    await conn.execute(f"SET plan_cache_mode = {mode}")
    async with contextlib.AsyncExitStack() as stack:
        await conn.execute(f"PREPARE {STATEMENT} AS {sql}")
        stack.push_async_callback(conn.execute, f"DEALLOCATE {STATEMENT}")
        call = await execute_call(conn, args)
        for _ in range(WARMUP_EXECUTIONS):
            await rolled_back(conn, call)
        before = await plan_counts(conn)
        plans = [
            parse_plan(await rolled_back(conn, f"EXPLAIN (ANALYZE, FORMAT JSON) {call}"))
            for _ in range(EXPLAIN_RUNS)
        ]
        after = await plan_counts(conn)
    fastest = min(plans, key=lambda plan: number(plan, "Execution Time"))
    root = cell(fastest, "Plan", dict)
    jit = fastest.get("JIT")
    jit_ms = number(cell(jit, "Timing", dict), "Total") if isinstance(jit, dict) else None
    used = [key.removesuffix("_plans") for key in after if after[key] > before[key]]
    return PlanRecord(
        concurrency_limit=limit,
        rows=rows,
        mode=mode,
        chosen_plan="+".join(used) if mode == "auto" else None,
        total_cost=number(root, "Total Cost"),
        jit=jit_ms is not None,
        jit_ms=jit_ms,
        planning_ms=number(fastest, "Planning Time"),
        execution_ms=number(fastest, "Execution Time"),
        claimed_rows=int(number(root, "Actual Rows")),
    )


async def ramp(
    names: list[QueueEntrypoint], queries: dict[int, ComposedQuery], steps: list[int]
) -> list[PlanRecord]:
    tables = DBSettings().qualified
    # statement_cache_size=0: DEALLOCATE would otherwise break asyncpg's own cached statements.
    async with contextlib.AsyncExitStack() as stack:
        conn = await asyncpg.connect(statement_cache_size=0)
        stack.push_async_callback(conn.close)
        if await conn.fetchval("SELECT to_regclass($1)", tables.queue_table) is None:
            raise SystemExit(f"{tables.queue_table} does not exist; run `pgq install` first")
        for setting in ("server_version", "jit", "jit_above_cost"):
            print(f"{setting} = {await conn.fetchval('SELECT current_setting($1)', setting)}")
        await conn.execute(f"TRUNCATE {tables.queue_table}, {tables.queue_table_log}")
        records: list[PlanRecord] = []
        queued = 0
        for target in steps:
            await conn.execute(
                f"""INSERT INTO {tables.queue_table} (priority, entrypoint, status)
                SELECT 0, ($1::text[])[1 + series % $2], 'queued'
                FROM generate_series(1, $3) AS series""",
                names,
                len(names),
                target - queued,
            )
            queued = target
            await conn.execute(f"ANALYZE {tables.queue_table}")
            records.extend(
                [
                    await explain_mode(conn, limit, target, mode, query)
                    for limit, query in queries.items()
                    for mode in get_args(PlanMode)
                ]
            )
    return records


@app.command(help="Ramp the queue and EXPLAIN ANALYZE the dequeue. TRUNCATES the queue and log.")
def main(
    entrypoints: int = typer.Option(27, min=1),
    concurrency_limits: str = typer.Option(
        "1,5,50", help="Per-entrypoint limits to sweep; 0 = none."
    ),
    max_concurrent_tasks: int = typer.Option(100, min=0, help="Global limit; 0 = none."),
    batch_size: int = typer.Option(10, min=1),
    steps: str = typer.Option(
        "0,10000,100000,250000,500000,1000000", help="Queued rows after each step."
    ),
    output_json: Path | None = typer.Option(None),
) -> None:
    totals = [int(step) for step in steps.split(",")]
    if totals != sorted(totals) or totals[0] < 0:
        raise typer.BadParameter("steps must be non-negative and non-decreasing")
    limits = [int(limit) for limit in concurrency_limits.split(",")]
    if min(limits) < 0:
        raise typer.BadParameter("concurrency limits must be non-negative")
    names = [QueueEntrypoint(f"ep{index}") for index in range(entrypoints)]
    builder = QueryQueueBuilder(DBSettings())
    queries = {
        limit: builder.build_dequeue_query(
            batch_size=batch_size,
            entrypoints=names,
            concurrency_limits=[limit] * entrypoints,
            queue_manager_id=QueueManagerId(uuid.uuid4()),
            global_concurrency_limit=max_concurrent_tasks or None,
            heartbeat_timeout=timedelta(seconds=30),
        )
        for limit in limits
    }
    records = [dataclasses.asdict(record) for record in asyncio.run(ramp(names, queries, totals))]
    print(tabulate(records, headers="keys"))
    if output_json:
        output_json.write_text(json.dumps(records, indent=2))


if __name__ == "__main__":
    app()
