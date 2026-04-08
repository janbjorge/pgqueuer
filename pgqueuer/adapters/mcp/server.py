"""PgQueuer MCP server — read-only insights into queue state and statistics."""

from __future__ import annotations

import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Annotated

import asyncpg
from mcp.server.fastmcp import Context, FastMCP
from mcp.server.session import ServerSession

from pgqueuer.adapters.persistence.qb import (
    DBSettings,
    QueryQueueBuilder,
    QuerySchedulerBuilder,
)


class PgQueuerDatabase:
    """Thin wrapper holding a connection pool and query builders."""

    def __init__(self, pool: asyncpg.Pool, settings: DBSettings) -> None:
        self.pool = pool
        self.settings = settings
        self.qbq = QueryQueueBuilder(settings)
        self.qbs = QuerySchedulerBuilder(settings)

    async def fetch(self, sql: str, *args: object) -> list[dict[str, object]]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(sql, *args)
            return [dict(row) for row in rows]


Ctx = Context[ServerSession, PgQueuerDatabase, object]

# ---------------------------------------------------------------------------
# Annotated parameter types — descriptions surface in the MCP tool schema.
# These are the ONLY knobs an agent can turn; keep them simple and bounded.
# ---------------------------------------------------------------------------

Limit = Annotated[
    int,
    "Maximum number of rows to return. Must be a positive integer. "
    "Default 50. Higher values return more history but increase response size.",
]
TimePeriod = Annotated[
    str,
    "ISO-8601 duration for the look-back window. "
    "Examples: 'PT5M' (5 minutes), 'PT1H' (1 hour), 'P1D' (1 day), 'P7D' (7 days). "
    "If omitted or null, no time filter is applied and all available data is returned.",
]
StaleThreshold = Annotated[
    str,
    "ISO-8601 duration defining when a picked job is considered stale. "
    "A job whose heartbeat is older than NOW() minus this threshold is stale. "
    "Examples: 'PT5M' (5 minutes), 'PT30M' (30 minutes). Default: 'PT5M'.",
]
Limit = Annotated[
    int,
    "Maximum number of rows to return. Must be a positive integer. Default 100.",
]
Offset = Annotated[
    int,
    "Number of rows to skip before returning results. Use for pagination. Default 0.",
]


def _parse_interval(period: str | None) -> timedelta | None:
    """Parse a simple ISO-8601 duration string into a timedelta, or return None."""
    if not period:
        return None
    p = period.upper().strip()
    m = re.match(r"P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)?$", p)
    if not m:
        raise ValueError(f"Cannot parse duration: {period!r}. Use ISO-8601 like 'PT1H' or 'P7D'.")
    days, hours, minutes, seconds = (int(g) if g else 0 for g in m.groups())
    total = timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)
    if total <= timedelta():
        raise ValueError(f"Duration must be positive, got: {period!r}")
    return total


def _db(ctx: Ctx) -> PgQueuerDatabase:
    return ctx.request_context.lifespan_context


def _register_tools(mcp: FastMCP) -> None:  # noqa: C901
    """Register all read-only insight tools onto the given FastMCP instance."""

    # ===================================================================
    # QUEUE OVERVIEW
    # ===================================================================

    @mcp.tool()
    async def queue_size(ctx: Ctx) -> list[dict[str, object]]:
        """Snapshot of current queue depth, grouped by entrypoint, status, and priority.

        This is the first tool you should call when investigating queue health.
        It tells you how many jobs are waiting (queued), being processed (picked),
        or in other states, broken down by job type (entrypoint) and priority level.

        Returned columns:
          - count:      number of jobs in this group
          - entrypoint: the job handler name (e.g. "send_email", "process_payment")
          - status:     one of 'queued', 'picked', 'successful', 'exception',
                        'canceled', 'deleted'
          - priority:   integer priority (higher = processed first)

        This tool takes no parameters and always returns the current state.
        An empty result means the queue is completely empty (no pending or
        in-progress jobs).

        Common patterns:
          - Large 'queued' counts with few 'picked': workers may be down or slow.
          - Many 'picked' jobs with stale heartbeats: workers may be stuck
            (use stale_jobs to investigate).
        """
        d = _db(ctx)
        return await d.fetch(d.qbq.build_queue_size_query())

    @mcp.tool()
    async def queue_table_info(
        ctx: Ctx,
        limit: Limit = 100,
        offset: Offset = 0,
    ) -> list[dict[str, object]]:
        """Browse raw queue table rows ordered by priority (descending) then id.

        Use this for deep inspection of individual jobs — their payloads,
        headers, timestamps, and which worker owns them.

        Returned columns:
          - id:               unique job identifier (serial integer)
          - priority:         integer priority (higher = dequeued first)
          - queue_manager_id: UUID of the worker that picked this job (null if queued)
          - created:          when the job was enqueued (timestamptz)
          - updated:          last status change (timestamptz)
          - heartbeat:        last heartbeat from the worker (timestamptz)
          - execute_after:    earliest time the job can be dequeued (timestamptz)
          - status:           'queued' or 'picked' (jobs leave the table on completion)
          - entrypoint:       job handler name
          - dedupe_key:       optional deduplication key (null if not set)
          - payload:          job payload as bytes (null if not set)
          - headers:          JSONB tracing/metadata headers (null if not set)

        Parameters:
          - limit:  max rows to return (default 100)
          - offset: skip this many rows first (default 0, for pagination)

        Note: Only jobs currently in the queue are shown. Completed, failed,
        and canceled jobs are moved to the log table — use queue_log or
        failed_jobs to see those.
        """
        d = _db(ctx)
        return await d.fetch(d.qbq.build_queue_table_browse_query(), limit, offset)

    # ===================================================================
    # STATISTICS & THROUGHPUT
    # ===================================================================

    @mcp.tool()
    async def queue_stats(
        ctx: Ctx,
        period: TimePeriod | None = None,
        limit: Limit = 50,
    ) -> list[dict[str, object]]:
        """Aggregated job processing statistics bucketed by second.

        Shows how many jobs transitioned to each status per second, grouped
        by entrypoint and priority. Use this to understand throughput trends,
        spot processing spikes, or confirm that workers are making progress.

        Before returning results, this tool aggregates any pending log entries
        into the statistics table (a lightweight roll-up operation).

        Returned columns:
          - count:      number of jobs in this time bucket
          - created:    the second-level time bucket (timestamptz)
          - entrypoint: job handler name
          - priority:   integer priority level
          - status:     the status jobs transitioned to ('queued', 'picked',
                        'successful', 'exception', 'canceled', 'deleted')

        Parameters:
          - period: ISO-8601 duration to look back (e.g. 'PT1H' for the last hour).
                    If omitted, returns all available history.
          - limit:  max rows to return, most recent first (default 50).

        Example use cases:
          - "How many jobs succeeded in the last hour?" -> period='PT1H'
          - "Show me the last 200 stats entries" -> limit=200
        """
        d = _db(ctx)
        await d.fetch(d.qbq.build_aggregate_log_data_to_statistics_query())
        interval = _parse_interval(period)
        return await d.fetch(d.qbq.build_log_statistics_query(), limit, interval)

    @mcp.tool()
    async def throughput_summary(
        ctx: Ctx,
        period: TimePeriod | None = None,
    ) -> list[dict[str, object]]:
        """High-level throughput summary: total jobs per entrypoint and status.

        Unlike queue_stats (which returns per-second buckets), this tool returns
        a single aggregated row per entrypoint+status combination — much easier
        to read when you just want totals.

        Returned columns:
          - entrypoint:  job handler name
          - status:      'queued', 'picked', 'successful', 'exception',
                         'canceled', or 'deleted'
          - total_count: total number of jobs that reached this status

        Parameters:
          - period: ISO-8601 duration to look back (e.g. 'P1D' for the last day).
                    If omitted, returns totals across all available history.

        Example use cases:
          - "What's the success/failure ratio for send_email today?"
            -> period='P1D', then compare successful vs exception counts
          - "Which entrypoint has the most failures this week?"
            -> period='P7D', look for highest exception total_count
        """
        d = _db(ctx)
        interval = _parse_interval(period)
        return await d.fetch(d.qbq.build_throughput_summary_query(), interval)

    # ===================================================================
    # FAILURE INVESTIGATION
    # ===================================================================

    @mcp.tool()
    async def failed_jobs(
        ctx: Ctx,
        limit: Limit = 50,
    ) -> list[dict[str, object]]:
        """Recent jobs that failed with an exception, most recent first.

        Use this when investigating errors. Each row includes the full
        traceback as a JSON object so you can see exactly what went wrong.

        Returned columns:
          - id:         log entry identifier
          - created:    when the failure was recorded (timestamptz)
          - job_id:     the original job id from the queue table
          - status:     always 'exception' for this tool
          - priority:   the job's priority level
          - entrypoint: the job handler that failed
          - traceback:  JSON object with fields:
              - job_id:             same as the row's job_id
              - timestamp:          when the exception occurred
              - exception_type:     e.g. "ValueError", "ConnectionError"
              - exception_message:  the str() of the exception
              - traceback:          full Python traceback as a string
              - additional_context: optional dict with extra info (may be null)
          - aggregated: whether this entry has been rolled up into statistics

        Parameters:
          - limit: max rows to return (default 50)

        Tip: If you see many failures for one entrypoint, check the traceback
        field for the root cause. Common patterns include connection errors
        (downstream service down), serialization errors (bad payload), or
        business logic exceptions.
        """
        d = _db(ctx)
        return await d.fetch(d.qbq.build_failed_jobs_query(), limit)

    # ===================================================================
    # EVENT LOG
    # ===================================================================

    @mcp.tool()
    async def queue_log(
        ctx: Ctx,
        limit: Limit = 100,
    ) -> list[dict[str, object]]:
        """Full event log showing every job state transition, most recent first.

        Every time a job changes state (enqueued, picked by a worker, completed,
        failed, canceled, deleted), a row is written to this log. This gives you
        a complete audit trail of job lifecycle events.

        Returned columns:
          - id:         log entry identifier (monotonically increasing)
          - created:    when this state transition happened (timestamptz)
          - job_id:     the job's id in the queue table
          - status:     the status the job transitioned TO: 'queued' (enqueued),
                        'picked' (claimed by worker), 'successful' (completed ok),
                        'exception' (failed), 'canceled', 'deleted'
          - priority:   the job's priority level
          - entrypoint: the job handler name
          - traceback:  JSON traceback (only present for 'exception' status, else null)
          - aggregated: whether this entry has been rolled into the statistics table

        Parameters:
          - limit: max rows to return (default 100)

        Note: This log can grow large. Use a reasonable limit. For failure
        investigation specifically, prefer the failed_jobs tool which filters
        to exceptions only.
        """
        d = _db(ctx)
        return await d.fetch(d.qbq.build_queue_log_query(), limit)

    # ===================================================================
    # SCHEDULES (CRON)
    # ===================================================================

    @mcp.tool()
    async def schedules(ctx: Ctx) -> list[dict[str, object]]:
        """All cron-based recurring task definitions and their current state.

        PgQueuer supports cron-style scheduled tasks. This tool lists every
        registered schedule so you can verify they are running on time.

        Returned columns:
          - id:         schedule identifier
          - expression: cron expression (e.g. '*/5 * * * *' = every 5 minutes)
          - heartbeat:  last heartbeat from the worker handling this schedule
          - created:    when the schedule was registered
          - updated:    last state change
          - next_run:   when this schedule will next fire (timestamptz)
          - last_run:   when this schedule last completed (null if never run)
          - status:     'queued' (waiting for next_run) or 'picked' (currently executing)
          - entrypoint: the scheduled task handler name

        This tool takes no parameters and returns all schedules.

        Diagnosis patterns:
          - next_run is far in the past but status is 'queued':
            no worker is picking up this schedule.
          - status is 'picked' and heartbeat is stale:
            the worker executing this schedule may be stuck.
          - last_run is null: the schedule has never executed since registration.
        """
        d = _db(ctx)
        return await d.fetch(d.qbs.build_peek_schedule_query())

    # ===================================================================
    # WORKER HEALTH
    # ===================================================================

    @mcp.tool()
    async def stale_jobs(
        ctx: Ctx,
        threshold: StaleThreshold = "PT5M",
        limit: Limit = 50,
    ) -> list[dict[str, object]]:
        """Jobs stuck in 'picked' status whose heartbeat is older than the threshold.

        Workers send periodic heartbeats while processing a job. If a job's
        heartbeat falls behind, the worker may have crashed, been killed, or
        is hanging. These stale jobs block queue throughput and may need
        manual intervention or will be retried if retry_after is configured.

        Returned columns:
          - id:                      job identifier
          - priority:                job priority
          - queue_manager_id:        UUID of the worker that claimed this job
          - created:                 when the job was enqueued
          - updated:                 last status change
          - heartbeat:               last heartbeat timestamp
          - execute_after:           scheduled execution time
          - status:                  always 'picked' for this tool
          - entrypoint:              job handler name
          - seconds_since_heartbeat: how many seconds since the last heartbeat
                                     (the higher this number, the more likely
                                     the worker is dead)

        Parameters:
          - threshold: ISO-8601 duration defining staleness (default 'PT5M' = 5 min).
                       Jobs with heartbeat older than NOW() - threshold are returned.
          - limit:     max rows to return (default 50)

        Example: To find jobs stuck for over 30 minutes, use threshold='PT30M'.

        If you see stale jobs, cross-reference the queue_manager_id with
        active_workers to check if that worker is still alive.
        """
        d = _db(ctx)
        interval = _parse_interval(threshold)
        return await d.fetch(d.qbq.build_stale_jobs_query(), interval, limit)

    @mcp.tool()
    async def active_workers(ctx: Ctx) -> list[dict[str, object]]:
        """Workers currently processing jobs, grouped by worker UUID.

        Each PgQueuer worker instance has a unique queue_manager_id (UUID).
        This tool shows which workers are active and what they're working on.

        Returned columns:
          - queue_manager_id: unique worker UUID
          - active_jobs:      number of jobs this worker currently holds
          - oldest_heartbeat: the oldest heartbeat among this worker's jobs
                              (if very old, the worker might be stuck)
          - newest_heartbeat: the most recent heartbeat
          - entrypoints:      array of distinct entrypoint names this worker
                              is processing

        This tool takes no parameters and returns one row per active worker.
        An empty result means no workers are currently processing any jobs.

        Diagnosis patterns:
          - Many workers but low throughput: jobs may be slow or workers
            are hitting concurrency limits.
          - One worker with a very old oldest_heartbeat: that worker may
            be stuck on a long-running or hung job. Use stale_jobs to find
            the specific job.
          - No workers at all but queue_size shows 'queued' jobs: workers
            are not running or cannot connect.
        """
        d = _db(ctx)
        return await d.fetch(d.qbq.build_active_workers_query())

    @mcp.tool()
    async def queue_age(ctx: Ctx) -> list[dict[str, object]]:
        """Age of the oldest waiting job per entrypoint — measures backlog depth.

        Shows how long jobs have been sitting in 'queued' status without being
        picked up. A growing age indicates workers can't keep up with the
        inflow for that entrypoint.

        Returned columns:
          - entrypoint:         job handler name
          - queued_count:       total number of jobs waiting in 'queued' status
          - oldest_created:     timestamp of the oldest waiting job
          - oldest_age_seconds: seconds since the oldest job was created
                                (the key metric — higher = worse backlog)
          - avg_age_seconds:    average age of all queued jobs for this entrypoint

        This tool takes no parameters. Returns one row per entrypoint that
        has queued jobs, ordered by oldest_age_seconds descending (worst first).
        An empty result means no jobs are waiting — the queue is fully caught up.

        Diagnosis patterns:
          - oldest_age_seconds > 300 (5 min): workers may need scaling.
          - queued_count is high but avg_age is low: burst of recent jobs,
            workers will likely catch up.
          - Both count and age are high: sustained backlog, need more workers
            or faster processing.
        """
        d = _db(ctx)
        return await d.fetch(d.qbq.build_queue_age_query())

    # ===================================================================
    # SCHEMA & SETUP
    # ===================================================================

    @mcp.tool()
    async def schema_info(ctx: Ctx) -> list[dict[str, object]]:
        """PgQueuer table metadata: existence, durability mode, size, and row estimates.

        Use this to verify that PgQueuer is installed correctly and to check
        the durability configuration of each table.

        Returned columns:
          - table_name:     name of the PgQueuer table
          - persistence:    'LOGGED' (crash-safe, WAL-backed) or
                            'UNLOGGED' (faster but data lost on crash)
          - estimated_rows: approximate row count from pg_class
                            (updated by ANALYZE/autovacuum, may be stale)
          - total_size:     human-readable total disk usage including indexes

        This tool takes no parameters. Returns one row per PgQueuer table.

        Expected tables (with default naming):
          - pgqueuer:            the main job queue
          - pgqueuer_log:        event log of all state transitions
          - pgqueuer_statistics: aggregated throughput stats
          - pgqueuer_schedules:  cron schedule definitions

        If any table is missing from the result, PgQueuer may not be fully
        installed. Use the CLI 'pgq install' or 'pgq verify --expect present'
        to check.

        The persistence column is important: UNLOGGED tables are faster but
        will be truncated after an unclean PostgreSQL shutdown (crash, OOM kill).
        Production systems typically use 'balanced' or 'durable' durability.
        """
        d = _db(ctx)
        return await d.fetch(d.qbq.build_schema_info_query())


def create_mcp_server(
    dsn: str | None = None,
    settings: DBSettings = DBSettings(),
) -> FastMCP:
    """Factory that builds a fully-configured PgQueuer MCP server.

    Args:
        dsn: PostgreSQL connection string. If None, asyncpg reads standard
             libpq environment variables (PGHOST, PGPORT, PGUSER,
             PGPASSWORD, PGDATABASE) automatically.
        settings: PgQueuer DBSettings (table names, channel, etc.).
    """

    @asynccontextmanager
    async def app_lifespan(server: FastMCP) -> AsyncIterator[PgQueuerDatabase]:
        async with asyncpg.create_pool(dsn=dsn, min_size=1, max_size=5) as pool:
            yield PgQueuerDatabase(pool, settings)

    mcp = FastMCP("pgqueuer", lifespan=app_lifespan)

    @mcp.prompt()
    def pgqueuer_guide() -> str:
        """Suggested workflow for inspecting a PgQueuer installation."""
        return """\
You are connected to a PostgreSQL database running PgQueuer via MCP tools.
All tools are read-only and use predefined queries — they cannot modify data.

Recommended investigation workflow:

1. START HERE:
   `queue_size`       -- current job counts by entrypoint/status/priority.
   `schema_info`      -- verify tables exist, check durability and sizes.

2. THROUGHPUT:
   `throughput_summary` -- total jobs per entrypoint and status (with time filter).
   `queue_stats`        -- per-second time-series of job state transitions.

3. FAILURES:
   `failed_jobs`      -- recent exceptions with full Python tracebacks.

4. BACKLOG:
   `queue_age`        -- how old are the oldest waiting jobs per entrypoint?
   `queue_table_info` -- browse raw queue rows for deep inspection.

5. WORKER HEALTH:
   `active_workers`   -- which workers are alive and what are they doing?
   `stale_jobs`       -- jobs stuck in 'picked' with no recent heartbeat.

6. SCHEDULES:
   `schedules`        -- cron task definitions, next/last run times.

7. AUDIT:
   `queue_log`        -- full event log of every job state transition.

Key concepts:
- Jobs flow: queued -> picked -> successful/exception/canceled/deleted
- Workers heartbeat periodically while processing; stale heartbeats = stuck worker
- The queue table only contains active jobs (queued/picked); completed jobs
  move to the log table.
- Statistics are aggregated from the log into per-second buckets.
"""

    _register_tools(mcp)

    return mcp
