"""The dequeue statement, rendered once per concurrency-gate shape."""

from __future__ import annotations

import dataclasses
from datetime import timedelta
from enum import Enum
from typing import Literal, NamedTuple, assert_never

from pgqueuer.adapters.persistence.composer import Cte, QueryTemplate, TemplateComposer
from pgqueuer.domain.settings import QualifiedNames
from pgqueuer.domain.types import QueueEntrypoint, QueueManagerId


class DequeueShape(Enum):
    """The concurrency gates a dequeue statement renders."""

    no_gates = "no_gates"
    entrypoint_gate = "entrypoint_gate"
    global_gate = "global_gate"
    both_gates = "both_gates"


def due(alias: str = "") -> str:
    """The one predicate for a job whose ``execute_after`` has been reached."""
    column = f"{alias}.execute_after" if alias else "execute_after"
    return f"{column} <= NOW()"


@dataclasses.dataclass(frozen=True, kw_only=True)
class DequeueArgs:
    """The values one dequeue call renders and binds."""

    batch_size: int
    entrypoints: list[QueueEntrypoint]
    concurrency_limits: list[int]
    queue_manager_id: QueueManagerId
    global_concurrency_limit: int | None
    heartbeat_timeout: timedelta

    def __post_init__(self) -> None:
        if len(self.entrypoints) != len(self.concurrency_limits):
            raise ValueError("entrypoints and concurrency_limits must be the same length")
        # batch_size is rendered into the SQL text, where a bool would render as True.
        if isinstance(self.batch_size, bool) or self.batch_size < 1:
            raise ValueError("batch_size must be a positive integer")


class DequeueCacheKey(NamedTuple):
    shape: DequeueShape
    batch_size: int


def dequeue_shape(args: DequeueArgs) -> DequeueShape:
    """Select the gates a call needs; an entrypoint limit of 0 means unlimited."""
    entrypoint_gated = any(limit > 0 for limit in args.concurrency_limits)
    budget_gated = args.global_concurrency_limit is not None
    if entrypoint_gated and budget_gated:
        return DequeueShape.both_gates
    if entrypoint_gated:
        return DequeueShape.entrypoint_gate
    if budget_gated:
        return DequeueShape.global_gate
    return DequeueShape.no_gates


@dataclasses.dataclass
class DequeueSql:
    """The dequeue CTEs: one method per shape, one method per CTE.

    A CTE method returns the CTE under the name later bodies use for it. The
    placeholders every shape needs are bound on construction; the entrypoint
    limits and the worker budget are bound by the shape methods that use them.
    """

    qualified: QualifiedNames
    batch_size: int
    composer: TemplateComposer[DequeueArgs] = dataclasses.field(
        init=False,
        default_factory=TemplateComposer[DequeueArgs],
    )
    entrypoints: str = dataclasses.field(init=False)
    queue_manager: str = dataclasses.field(init=False)
    heartbeat: str = dataclasses.field(init=False)

    def __post_init__(self) -> None:
        self.entrypoints = self.composer.param(lambda args: args.entrypoints)
        self.queue_manager = self.composer.param(lambda args: args.queue_manager_id)
        self.heartbeat = self.composer.param(lambda args: args.heartbeat_timeout)

    @property
    def queue_table(self) -> str:
        return self.qualified.queue_table

    @property
    def batch(self) -> str:
        # A literal, not a bind: the generic plan PostgreSQL caches for a prepared
        # statement cannot see a bound LIMIT and costs the whole backlog for it.
        return str(self.batch_size)

    def bind_limits(self) -> str:
        return self.composer.param(lambda args: args.concurrency_limits)

    def bind_budget(self) -> str:
        return self.composer.param(lambda args: args.global_concurrency_limit)

    def no_gates(self) -> list[Cte]:
        return [
            self.available(),
            self.next_queued(budget=None),
            self.next_stale(budget=None),
            self.eligible(budget=None),
            self.claimed(),
            self.log_pick(),
        ]

    def global_gate(self) -> list[Cte]:
        budget = self.bind_budget()
        return [
            self.available(),
            self.worker_load(budget),
            self.next_queued(budget),
            self.next_stale(budget),
            self.eligible(budget),
            self.claimed(),
            self.log_pick(),
        ]

    def entrypoint_gate(self) -> list[Cte]:
        limits = self.bind_limits()
        return [
            self.params(limits),
            self.picked(),
            self.available_with_capacity(),
            self.unlimited_claims(),
            self.limited_claims(),
            self.next_queued_from_claims(budget=None),
            self.next_stale(budget=None),
            self.eligible_with_slots(budget=None),
            self.free_slots(),
            self.slot_assignments(),
            self.claimed_with_slots(),
            self.log_pick(),
        ]

    def both_gates(self) -> list[Cte]:
        limits = self.bind_limits()
        budget = self.bind_budget()
        return [
            self.params(limits),
            self.picked(),
            self.available_with_capacity(),
            self.worker_load(budget),
            self.unlimited_claims(),
            self.limited_claims(),
            self.next_queued_from_claims(budget),
            self.next_stale(budget),
            self.eligible_with_slots(budget),
            self.free_slots(),
            self.slot_assignments(),
            self.claimed_with_slots(),
            self.log_pick(),
        ]

    @staticmethod
    def under_budget(budget: str | None, keyword: Literal["WHERE", "AND"]) -> str:
        """The worker-budget filter line; empty without a budget, so the line drops."""
        if budget is None:
            return ""
        return f"{keyword} (SELECT total FROM worker_load) < {budget}"

    def eligible_limit(self, budget: str | None) -> str:
        if budget is None:
            return self.batch
        return "(SELECT headroom FROM worker_load)"

    def params(self, limits: str) -> Cte:
        """Columns: entrypoint, concurrency_limit."""
        # The limit comes from the dequeuing worker's own registration, so the
        # fleet has to agree on it. Workers declaring different limits for one
        # entrypoint is unsupported; the highest one wins.
        return Cte(
            "params",
            f"""
                SELECT
                    UNNEST({self.entrypoints}::text[]) AS entrypoint,
                    UNNEST({limits}::bigint[]) AS concurrency_limit
            """,
            comment="Unpack per-entrypoint parameters; concurrency_limit 0 means unlimited.",
        )

    def picked(self) -> Cte:
        """Columns: entrypoint, total."""
        return Cte(
            "picked",
            f"""
                SELECT entrypoint, COUNT(*) AS total
                FROM {self.queue_table}
                WHERE queue_manager_id IS NOT NULL
                  AND entrypoint = ANY({self.entrypoints})
                GROUP BY entrypoint
            """,
            comment="Per-entrypoint count of picked jobs (global, all workers).",
        )

    def available(self) -> Cte:
        """Columns: entrypoint."""
        return Cte(
            "available",
            f"SELECT UNNEST({self.entrypoints}::text[]) AS entrypoint",
            comment="No entrypoint carries a concurrency limit; all are available.",
        )

    def available_with_capacity(self) -> Cte:
        """Columns: entrypoint, remaining (NULL when unlimited). Reads params, picked."""
        # Applying the gate here rather than per row keeps a saturated entrypoint
        # from scanning its queued backlog at all. remaining is NULL for unlimited
        # entrypoints; otherwise it caps how many rows one dequeue may pick, so a
        # single statement cannot exceed the entrypoint's concurrency_limit.
        return Cte(
            "available",
            """
                SELECT
                    params.entrypoint,
                    CASE
                        WHEN params.concurrency_limit <= 0 THEN NULL
                        ELSE params.concurrency_limit - COALESCE(picked.total, 0)
                    END AS remaining
                FROM params
                LEFT JOIN picked ON picked.entrypoint = params.entrypoint
                WHERE params.concurrency_limit <= 0
                   OR COALESCE(picked.total, 0) < params.concurrency_limit
            """,
            comment="Entrypoints with free capacity; remaining caps a single dequeue.",
        )

    def worker_load(self, budget: str) -> Cte:
        """Columns: total, headroom."""
        # headroom, not the total, is what stops a batch overshooting: the total
        # only says whether a dequeue may start, never how much it may take.
        return Cte(
            "worker_load",
            f"""
                SELECT
                    COUNT(*) AS total,
                    GREATEST(LEAST({self.batch}, {budget} - COUNT(*)), 0) AS headroom
                FROM {self.queue_table}
                WHERE queue_manager_id = {self.queue_manager}
                  AND entrypoint = ANY({self.entrypoints})
            """,
            comment="This worker's picked jobs, and how many more this batch may claim.",
        )

    def unlimited_claims(self) -> Cte:
        """Columns: id, priority, entrypoint. Reads available_with_capacity."""
        return Cte(
            "unlimited_claims",
            f"""
                SELECT job.id, job.priority, available.entrypoint
                FROM available
                CROSS JOIN LATERAL (
                    SELECT candidate.id, candidate.priority
                    FROM {self.queue_table} candidate
                    WHERE candidate.entrypoint = available.entrypoint
                      AND candidate.status = 'queued'
                      AND {due("candidate")}
                    ORDER BY candidate.priority DESC, candidate.id ASC
                    LIMIT {self.batch}
                    FOR UPDATE SKIP LOCKED
                ) job
                WHERE available.remaining IS NULL
            """,
            comment="Claims for entrypoints without a limit; lock straight down the backlog.",
        )

    def limited_claims(self) -> Cte:
        """Columns: id, priority, entrypoint. Reads available_with_capacity."""
        # Windowing before the lock keeps the lock node out from under a LIMIT,
        # where SKIP LOCKED slides down the backlog and past the cap (#761).
        # The planner cannot evaluate the LEAST cap and costs a share of the whole
        # backlog for it; the constant LIMIT beneath bounds the estimate by the batch.
        return Cte(
            "limited_claims",
            f"""
                SELECT job.id, job.priority, available.entrypoint
                FROM available
                CROSS JOIN LATERAL (
                    SELECT locked.id, locked.priority
                    FROM (
                        SELECT batched.id
                        FROM (
                            SELECT candidate.id, candidate.priority
                            FROM {self.queue_table} candidate
                            WHERE candidate.entrypoint = available.entrypoint
                              AND candidate.status = 'queued'
                              AND {due("candidate")}
                            ORDER BY candidate.priority DESC, candidate.id ASC
                            LIMIT {self.batch}
                        ) batched
                        ORDER BY batched.priority DESC, batched.id ASC
                        LIMIT LEAST({self.batch}, available.remaining)
                    ) capped
                    CROSS JOIN LATERAL (
                        SELECT target.id, target.priority
                        FROM {self.queue_table} target
                        WHERE target.id = capped.id
                          AND target.status = 'queued'
                          AND {due("target")}
                        FOR UPDATE OF target SKIP LOCKED
                    ) locked
                    ORDER BY locked.priority DESC, locked.id ASC
                    LIMIT LEAST({self.batch}, available.remaining)
                ) job
                WHERE available.remaining IS NOT NULL
            """,
            comment="Claims for limited entrypoints; the window is fixed before the lock.",
        )

    def next_queued(self, budget: str | None) -> Cte:
        """Columns: id, priority. Reads available."""
        return Cte(
            "next_queued",
            f"""
                SELECT job.id, job.priority
                FROM available
                CROSS JOIN LATERAL (
                    SELECT candidate.id, candidate.priority
                    FROM {self.queue_table} candidate
                    WHERE candidate.entrypoint = available.entrypoint
                      AND candidate.status = 'queued'
                      AND {due("candidate")}
                    ORDER BY candidate.priority DESC, candidate.id ASC
                    LIMIT {self.batch}
                    FOR UPDATE SKIP LOCKED
                ) job
                {self.under_budget(budget, "WHERE")}
                ORDER BY job.priority DESC, job.id ASC
                LIMIT {self.batch}
            """,
            comment="New queued jobs; LATERAL hits the (entrypoint, priority, id) index.",
        )

    def next_queued_from_claims(self, budget: str | None) -> Cte:
        """Columns: id, priority, entrypoint. Reads unlimited_claims, limited_claims."""
        return Cte(
            "next_queued",
            f"""
                SELECT job.id, job.priority, job.entrypoint
                FROM (
                    SELECT id, priority, entrypoint FROM unlimited_claims
                    UNION ALL
                    SELECT id, priority, entrypoint FROM limited_claims
                ) job
                {self.under_budget(budget, "WHERE")}
                ORDER BY job.priority DESC, job.id ASC
                LIMIT {self.batch}
            """,
            comment="Both claim sets merged into one priority order.",
        )

    def next_stale(self, budget: str | None) -> Cte:
        """Columns: id, priority, entrypoint."""
        # The entrypoint concurrency gate is intentionally omitted here: re-picking a
        # stale job only transfers ownership of a row already counted in `picked`, so
        # net live execution is unchanged. Applying the gate would deadlock recovery
        # once leaked rows fill the slot.
        return Cte(
            "next_stale",
            f"""
                SELECT stale.id, stale.priority, stale.entrypoint
                FROM {self.queue_table} stale
                WHERE stale.status = 'picked'
                  AND stale.entrypoint = ANY({self.entrypoints})
                  AND stale.heartbeat < NOW() - {self.heartbeat}::interval
                  AND {due("stale")}
                  {self.under_budget(budget, "AND")}
                ORDER BY stale.priority DESC, stale.id ASC
                FOR UPDATE SKIP LOCKED
                LIMIT {self.batch}
            """,
            comment="Stale picked jobs whose heartbeat timed out.",
        )

    def eligible(self, budget: str | None) -> Cte:
        """Columns: id. Reads next_queued, next_stale."""
        return Cte(
            "eligible",
            f"""
                SELECT id FROM (
                    SELECT id, priority FROM next_queued
                    UNION ALL
                    SELECT id, priority FROM next_stale
                ) combined
                ORDER BY priority DESC, id ASC
                LIMIT {self.eligible_limit(budget)}
            """,
            comment="Merge both sets into one priority order so stale competes with fresh.",
        )

    def eligible_with_slots(self, budget: str | None) -> Cte:
        """Columns: id, priority, entrypoint, fresh. Reads next_queued, next_stale."""
        # A stale re-pick keeps the slot it already holds; only fresh claims need one.
        return Cte(
            "eligible",
            f"""
                SELECT id, priority, entrypoint, fresh FROM (
                    SELECT id, priority, entrypoint, TRUE AS fresh FROM next_queued
                    UNION ALL
                    SELECT id, priority, entrypoint, FALSE AS fresh FROM next_stale
                ) combined
                ORDER BY priority DESC, id ASC
                LIMIT {self.eligible_limit(budget)}
            """,
            comment="Merge both sets into one priority order so stale competes with fresh.",
        )

    def free_slots(self) -> Cte:
        """Columns: entrypoint, slot, slot_rank. Reads available_with_capacity, params."""
        # The partial unique index on (entrypoint, slot) enforces the limit across
        # snapshots: a uniqueness check sees uncommitted tuples, so the loser of a
        # slot race fails with 23505 and retries rather than overshooting (#761).
        # Generating held+batch seats finds the lowest free ones a dequeue can use:
        # at most `held` seats inside that range are taken.
        return Cte(
            "free_slots",
            f"""
                SELECT
                    available.entrypoint,
                    slots.slot,
                    ROW_NUMBER() OVER (
                        PARTITION BY available.entrypoint ORDER BY slots.slot
                    ) AS slot_rank
                FROM available
                JOIN params ON params.entrypoint = available.entrypoint
                CROSS JOIN LATERAL GENERATE_SERIES(
                    0,
                    params.concurrency_limit - available.remaining
                        + LEAST({self.batch}, available.remaining) - 1
                ) AS slots(slot)
                WHERE available.remaining IS NOT NULL
                  AND NOT EXISTS (
                      SELECT FROM {self.queue_table} holder
                      WHERE holder.entrypoint = available.entrypoint
                        AND holder.status = 'picked'
                        AND holder.slot = slots.slot
                  )
            """,
            comment="Free capacity slots, ranked; only as many seats as one dequeue uses.",
        )

    def slot_assignments(self) -> Cte:
        """Columns: id, slot. Reads eligible_with_slots, free_slots."""
        return Cte(
            "slot_assignments",
            """
                SELECT ranked.id, free_slots.slot
                FROM (
                    SELECT
                        id,
                        entrypoint,
                        ROW_NUMBER() OVER (
                            PARTITION BY entrypoint ORDER BY priority DESC, id ASC
                        ) AS slot_rank
                    FROM eligible
                    WHERE fresh
                ) ranked
                JOIN free_slots
                    ON free_slots.entrypoint = ranked.entrypoint
                    AND free_slots.slot_rank = ranked.slot_rank
            """,
            comment="Pair each fresh claim with a free slot by rank; unpaired ones drop.",
        )

    def claimed(self) -> Cte:
        """Columns: every job column. Reads eligible."""
        return Cte(
            "claimed",
            f"""
                UPDATE {self.queue_table}
                SET status = 'picked',
                    updated   = NOW(),
                    heartbeat = NOW(),
                    -- Taking over another manager's stale row counts as a failed attempt.
                    attempts  = attempts + CASE WHEN status = 'picked'
                        AND queue_manager_id IS DISTINCT FROM {self.queue_manager}
                        THEN 1 ELSE 0 END,
                    queue_manager_id = {self.queue_manager}
                WHERE id IN (SELECT id FROM eligible)
                RETURNING *
            """,
            comment="Claim every eligible job in one atomic UPDATE.",
        )

    def claimed_with_slots(self) -> Cte:
        """Columns: every job column. Reads eligible_with_slots, slot_assignments, params."""
        return Cte(
            "claimed",
            f"""
                UPDATE {self.queue_table} job
                SET status = 'picked',
                    updated   = NOW(),
                    heartbeat = NOW(),
                    -- Taking over another manager's stale row counts as a failed attempt.
                    attempts  = job.attempts + CASE WHEN job.status = 'picked'
                        AND job.queue_manager_id IS DISTINCT FROM {self.queue_manager}
                        THEN 1 ELSE 0 END,
                    queue_manager_id = {self.queue_manager},
                    slot = CASE WHEN eligible.fresh THEN slot_assignments.slot ELSE job.slot END
                FROM eligible
                LEFT JOIN slot_assignments ON slot_assignments.id = eligible.id
                LEFT JOIN params ON params.entrypoint = eligible.entrypoint
                WHERE job.id = eligible.id
                  AND (NOT eligible.fresh
                       OR params.concurrency_limit <= 0
                       OR slot_assignments.slot IS NOT NULL)
                RETURNING job.*
            """,
            comment="Claim every eligible job in one atomic UPDATE.",
        )

    def log_pick(self) -> Cte:
        """Reads claimed."""
        return Cte(
            "log_pick",
            f"""
                INSERT INTO {self.qualified.queue_table_log} (job_id, status, entrypoint, priority)
                SELECT id, status, entrypoint, priority FROM claimed
            """,
            comment="Record the pick in the log table.",
        )


def render_dequeue(qualified: QualifiedNames, key: DequeueCacheKey) -> QueryTemplate[DequeueArgs]:
    sql = DequeueSql(qualified, key.batch_size)
    shape = key.shape
    if shape is DequeueShape.no_gates:
        ctes = sql.no_gates()
    elif shape is DequeueShape.entrypoint_gate:
        ctes = sql.entrypoint_gate()
    elif shape is DequeueShape.global_gate:
        ctes = sql.global_gate()
    elif shape is DequeueShape.both_gates:
        ctes = sql.both_gates()
    else:
        assert_never(shape)
    return sql.composer.render(ctes, "SELECT * FROM claimed ORDER BY priority DESC, id ASC;")
