"""The dequeue statement, composed from the concurrency gates in use."""

from __future__ import annotations

import dataclasses
from datetime import timedelta
from typing import NamedTuple

from pgqueuer.adapters.persistence.composer import ComposedQuery, Cte, SqlComposer
from pgqueuer.domain.settings import QualifiedNames
from pgqueuer.domain.types import QueueEntrypoint, QueueManagerId


def due(alias: str = "") -> str:
    """The one predicate for a job whose ``execute_after`` has been reached."""
    column = f"{alias}.execute_after" if alias else "execute_after"
    return f"{column} <= NOW()"


class DequeueShape(NamedTuple):
    capacity_gated: bool
    budget_gated: bool
    batch_size: int


class EntrypointGate(NamedTuple):
    """Placeholder for the per-entrypoint limits, parallel to the entrypoints."""

    limits: str


class WorkerBudget(NamedTuple):
    """Placeholder for the worker's ``global_concurrency_limit``."""

    limit: str


class DequeueBinds(NamedTuple):
    """Placeholders of one dequeue; a gate that is not configured is ``None``."""

    entrypoints: str
    queue_manager: str
    heartbeat: str
    gate: EntrypointGate | None
    budget: WorkerBudget | None

    @classmethod
    def bind(
        cls,
        composer: SqlComposer,
        *,
        entrypoints: list[QueueEntrypoint],
        concurrency_limits: list[int],
        queue_manager_id: QueueManagerId,
        global_concurrency_limit: int | None,
        heartbeat_timeout: timedelta,
    ) -> DequeueBinds:
        """Bind in one fixed order, so SQL cached per shape pairs with fresh args."""
        bound_entrypoints = composer.bind(list(entrypoints))
        queue_manager = composer.bind(queue_manager_id)
        heartbeat = composer.bind(heartbeat_timeout)
        gate = None
        if any(limit > 0 for limit in concurrency_limits):
            gate = EntrypointGate(composer.bind(list(concurrency_limits)))
        budget = None
        if global_concurrency_limit is not None:
            budget = WorkerBudget(composer.bind(global_concurrency_limit))
        return cls(bound_entrypoints, queue_manager, heartbeat, gate, budget)


@dataclasses.dataclass(frozen=True)
class WorkerLoad:
    """The ``worker_load`` CTE and the two expressions other CTEs read it through."""

    cte: Cte
    under_budget: str
    headroom: str


class DequeueStatement(NamedTuple):
    ctes: tuple[Cte, ...]
    final: str


@dataclasses.dataclass(frozen=True)
class DequeueFragments:
    """One method per CTE, and one method per shape chaining them.

    A fragment takes the CTEs it reads as arguments, so a shape method is the
    statement's data flow, read top to bottom. Every body is the SQL it renders.
    """

    qualified: QualifiedNames
    binds: DequeueBinds
    # A literal, not a bind: the generic plan PostgreSQL caches for a prepared
    # statement cannot see a bound LIMIT and costs the whole backlog for it.
    batch: str

    @property
    def queue_table(self) -> str:
        return self.qualified.queue_table

    def no_gates(self) -> DequeueStatement:
        available = self.all_available()
        queued = self.next_queued(available, load=None)
        stale = self.next_stale(load=None, carry_entrypoint=False)
        eligible = self.eligible(queued, stale, load=None)
        claimed = self.claim(eligible)
        return DequeueStatement(
            (available, queued, stale, eligible, claimed, self.log_pick(claimed)),
            self.select_claimed(claimed),
        )

    def global_gate(self, budget: WorkerBudget) -> DequeueStatement:
        available = self.all_available()
        load = self.worker_load(budget)
        queued = self.next_queued(available, load)
        stale = self.next_stale(load, carry_entrypoint=False)
        eligible = self.eligible(queued, stale, load)
        claimed = self.claim(eligible)
        return DequeueStatement(
            (available, load.cte, queued, stale, eligible, claimed, self.log_pick(claimed)),
            self.select_claimed(claimed),
        )

    def entrypoint_gate(self, gate: EntrypointGate) -> DequeueStatement:
        params = self.params(gate)
        picked = self.picked()
        available = self.gated_available(params, picked)
        unlimited = self.unlimited_claims(available)
        limited = self.limited_claims(available)
        queued = self.merged_next_queued(unlimited, limited, load=None)
        stale = self.next_stale(load=None, carry_entrypoint=True)
        eligible = self.slotted_eligible(queued, stale, load=None)
        free_slots = self.free_slots(available, params)
        assignments = self.slot_assignments(eligible, free_slots)
        claimed = self.slotted_claim(eligible, assignments, params)
        return DequeueStatement(
            (
                params,
                picked,
                available,
                unlimited,
                limited,
                queued,
                stale,
                eligible,
                free_slots,
                assignments,
                claimed,
                self.log_pick(claimed),
            ),
            self.select_claimed(claimed),
        )

    def both_gates(self, gate: EntrypointGate, budget: WorkerBudget) -> DequeueStatement:
        params = self.params(gate)
        picked = self.picked()
        available = self.gated_available(params, picked)
        load = self.worker_load(budget)
        unlimited = self.unlimited_claims(available)
        limited = self.limited_claims(available)
        queued = self.merged_next_queued(unlimited, limited, load)
        stale = self.next_stale(load, carry_entrypoint=True)
        eligible = self.slotted_eligible(queued, stale, load)
        free_slots = self.free_slots(available, params)
        assignments = self.slot_assignments(eligible, free_slots)
        claimed = self.slotted_claim(eligible, assignments, params)
        return DequeueStatement(
            (
                params,
                picked,
                available,
                load.cte,
                unlimited,
                limited,
                queued,
                stale,
                eligible,
                free_slots,
                assignments,
                claimed,
                self.log_pick(claimed),
            ),
            self.select_claimed(claimed),
        )

    def params(self, gate: EntrypointGate) -> Cte:
        """Columns: entrypoint, concurrency_limit."""
        # The limit comes from the dequeuing worker's own registration, so the
        # fleet has to agree on it. Workers declaring different limits for one
        # entrypoint is unsupported; the highest one wins.
        return Cte(
            "params",
            f"""
                SELECT
                    UNNEST({self.binds.entrypoints}::text[]) AS entrypoint,
                    UNNEST({gate.limits}::bigint[]) AS concurrency_limit
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
                  AND entrypoint = ANY({self.binds.entrypoints})
                GROUP BY entrypoint
            """,
            comment="Per-entrypoint count of picked jobs (global, all workers).",
        )

    def gated_available(self, params: Cte, picked: Cte) -> Cte:
        """Columns: entrypoint, remaining (NULL when the entrypoint is unlimited)."""
        # Applying the gate here rather than per row keeps a saturated entrypoint
        # from scanning its queued backlog at all. remaining is NULL for unlimited
        # entrypoints; otherwise it caps how many rows one dequeue may pick, so a
        # single statement cannot exceed the entrypoint's concurrency_limit.
        return Cte(
            "available",
            f"""
                SELECT
                    {params}.entrypoint,
                    CASE
                        WHEN {params}.concurrency_limit <= 0 THEN NULL
                        ELSE {params}.concurrency_limit - COALESCE({picked}.total, 0)
                    END AS remaining
                FROM {params}
                LEFT JOIN {picked} ON {picked}.entrypoint = {params}.entrypoint
                WHERE {params}.concurrency_limit <= 0
                   OR COALESCE({picked}.total, 0) < {params}.concurrency_limit
            """,
            comment="Entrypoints with free capacity; remaining caps a single dequeue.",
        )

    def all_available(self) -> Cte:
        """Columns: entrypoint."""
        return Cte(
            "available",
            f"SELECT UNNEST({self.binds.entrypoints}::text[]) AS entrypoint",
            comment="No entrypoint carries a concurrency limit; all are available.",
        )

    def worker_load(self, budget: WorkerBudget) -> WorkerLoad:
        """Columns: total, headroom."""
        # headroom, not the total, is what stops a batch overshooting: the total
        # only says whether a dequeue may start, never how much it may take.
        cte = Cte(
            "worker_load",
            f"""
                SELECT
                    COUNT(*) AS total,
                    GREATEST(LEAST({self.batch}, {budget.limit} - COUNT(*)), 0) AS headroom
                FROM {self.queue_table}
                WHERE queue_manager_id = {self.binds.queue_manager}
                  AND entrypoint = ANY({self.binds.entrypoints})
            """,
            comment="This worker's picked jobs, and how many more this batch may claim.",
        )
        return WorkerLoad(
            cte,
            under_budget=f"(SELECT total FROM {cte}) < {budget.limit}",
            headroom=f"(SELECT headroom FROM {cte})",
        )

    def unlimited_claims(self, available: Cte) -> Cte:
        """Columns: id, priority, entrypoint."""
        return Cte(
            "unlimited_claims",
            f"""
                SELECT job.id, job.priority, {available}.entrypoint
                FROM {available}
                CROSS JOIN LATERAL (
                    SELECT candidate.id, candidate.priority
                    FROM {self.queue_table} candidate
                    WHERE candidate.entrypoint = {available}.entrypoint
                      AND candidate.status = 'queued'
                      AND {due("candidate")}
                    ORDER BY candidate.priority DESC, candidate.id ASC
                    LIMIT {self.batch}
                    FOR UPDATE SKIP LOCKED
                ) job
                WHERE {available}.remaining IS NULL
            """,
            comment="Claims for entrypoints without a limit; lock straight down the backlog.",
        )

    def limited_claims(self, available: Cte) -> Cte:
        """Columns: id, priority, entrypoint."""
        # Windowing before the lock keeps the lock node out from under a LIMIT,
        # where SKIP LOCKED slides down the backlog and past the cap (#761).
        # The planner cannot evaluate the LEAST cap and costs a share of the whole
        # backlog for it; the constant LIMIT beneath bounds the estimate by the batch.
        cap = f"LEAST({self.batch}, {available}.remaining)"
        return Cte(
            "limited_claims",
            f"""
                SELECT job.id, job.priority, {available}.entrypoint
                FROM {available}
                CROSS JOIN LATERAL (
                    SELECT locked.id, locked.priority
                    FROM (
                        SELECT batched.id
                        FROM (
                            SELECT candidate.id, candidate.priority
                            FROM {self.queue_table} candidate
                            WHERE candidate.entrypoint = {available}.entrypoint
                              AND candidate.status = 'queued'
                              AND {due("candidate")}
                            ORDER BY candidate.priority DESC, candidate.id ASC
                            LIMIT {self.batch}
                        ) batched
                        ORDER BY batched.priority DESC, batched.id ASC
                        LIMIT {cap}
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
                    LIMIT {cap}
                ) job
                WHERE {available}.remaining IS NOT NULL
            """,
            comment="Claims for limited entrypoints; the window is fixed before the lock.",
        )

    def next_queued(self, available: Cte, load: WorkerLoad | None) -> Cte:
        """Columns: id, priority."""
        budget_where = f"WHERE {load.under_budget}" if load else ""
        return Cte(
            "next_queued",
            f"""
                SELECT job.id, job.priority
                FROM {available}
                CROSS JOIN LATERAL (
                    SELECT candidate.id, candidate.priority
                    FROM {self.queue_table} candidate
                    WHERE candidate.entrypoint = {available}.entrypoint
                      AND candidate.status = 'queued'
                      AND {due("candidate")}
                    ORDER BY candidate.priority DESC, candidate.id ASC
                    LIMIT {self.batch}
                    FOR UPDATE SKIP LOCKED
                ) job
                {budget_where}
                ORDER BY job.priority DESC, job.id ASC
                LIMIT {self.batch}
            """,
            comment="New queued jobs; LATERAL hits the (entrypoint, priority, id) index.",
        )

    def merged_next_queued(self, unlimited: Cte, limited: Cte, load: WorkerLoad | None) -> Cte:
        """Columns: id, priority, entrypoint."""
        budget_where = f"WHERE {load.under_budget}" if load else ""
        return Cte(
            "next_queued",
            f"""
                SELECT job.id, job.priority, job.entrypoint
                FROM (
                    SELECT id, priority, entrypoint FROM {unlimited}
                    UNION ALL
                    SELECT id, priority, entrypoint FROM {limited}
                ) job
                {budget_where}
                ORDER BY job.priority DESC, job.id ASC
                LIMIT {self.batch}
            """,
            comment="Both claim sets merged into one priority order.",
        )

    def next_stale(self, load: WorkerLoad | None, *, carry_entrypoint: bool) -> Cte:
        """Columns: id, priority, and entrypoint when ``carry_entrypoint``."""
        # The entrypoint concurrency gate is intentionally omitted here: re-picking a
        # stale job only transfers ownership of a row already counted in `picked`, so
        # net live execution is unchanged. Applying the gate would deadlock recovery
        # once leaked rows fill the slot.
        columns = "stale.id, stale.priority"
        if carry_entrypoint:
            columns = "stale.id, stale.priority, stale.entrypoint"
        budget_and = f"AND {load.under_budget}" if load else ""
        return Cte(
            "next_stale",
            f"""
                SELECT {columns}
                FROM {self.queue_table} stale
                WHERE stale.status = 'picked'
                  AND stale.entrypoint = ANY({self.binds.entrypoints})
                  AND stale.heartbeat < NOW() - {self.binds.heartbeat}::interval
                  AND {due("stale")}
                  {budget_and}
                ORDER BY stale.priority DESC, stale.id ASC
                FOR UPDATE SKIP LOCKED
                LIMIT {self.batch}
            """,
            comment="Stale picked jobs whose heartbeat timed out.",
        )

    def eligible(self, queued: Cte, stale: Cte, load: WorkerLoad | None) -> Cte:
        """Columns: id."""
        limit = load.headroom if load else self.batch
        return Cte(
            "eligible",
            f"""
                SELECT id FROM (
                    SELECT id, priority FROM {queued}
                    UNION ALL
                    SELECT id, priority FROM {stale}
                ) combined
                ORDER BY priority DESC, id ASC
                LIMIT {limit}
            """,
            comment="Merge both sets into one priority order so stale competes with fresh.",
        )

    def slotted_eligible(self, queued: Cte, stale: Cte, load: WorkerLoad | None) -> Cte:
        """Columns: id, priority, entrypoint, fresh."""
        # A stale re-pick keeps the slot it already holds; only fresh claims need one.
        limit = load.headroom if load else self.batch
        return Cte(
            "eligible",
            f"""
                SELECT id, priority, entrypoint, fresh FROM (
                    SELECT id, priority, entrypoint, TRUE AS fresh FROM {queued}
                    UNION ALL
                    SELECT id, priority, entrypoint, FALSE AS fresh FROM {stale}
                ) combined
                ORDER BY priority DESC, id ASC
                LIMIT {limit}
            """,
            comment="Merge both sets into one priority order so stale competes with fresh.",
        )

    def free_slots(self, available: Cte, params: Cte) -> Cte:
        """Columns: entrypoint, slot, slot_rank."""
        # The partial unique index on (entrypoint, slot) enforces the limit across
        # snapshots: a uniqueness check sees uncommitted tuples, so the loser of a
        # slot race fails with 23505 and retries rather than overshooting (#761).
        # Generating held+batch seats finds the lowest free ones a dequeue can use:
        # at most `held` seats inside that range are taken.
        return Cte(
            "free_slots",
            f"""
                SELECT
                    {available}.entrypoint,
                    slots.slot,
                    ROW_NUMBER() OVER (
                        PARTITION BY {available}.entrypoint ORDER BY slots.slot
                    ) AS slot_rank
                FROM {available}
                JOIN {params} ON {params}.entrypoint = {available}.entrypoint
                CROSS JOIN LATERAL GENERATE_SERIES(
                    0,
                    {params}.concurrency_limit - {available}.remaining
                        + LEAST({self.batch}, {available}.remaining) - 1
                ) AS slots(slot)
                WHERE {available}.remaining IS NOT NULL
                  AND NOT EXISTS (
                      SELECT FROM {self.queue_table} holder
                      WHERE holder.entrypoint = {available}.entrypoint
                        AND holder.status = 'picked'
                        AND holder.slot = slots.slot
                  )
            """,
            comment="Free capacity slots, ranked; only as many seats as one dequeue uses.",
        )

    def slot_assignments(self, eligible: Cte, free_slots: Cte) -> Cte:
        """Columns: id, slot."""
        return Cte(
            "slot_assignments",
            f"""
                SELECT ranked.id, {free_slots}.slot
                FROM (
                    SELECT
                        id,
                        entrypoint,
                        ROW_NUMBER() OVER (
                            PARTITION BY entrypoint ORDER BY priority DESC, id ASC
                        ) AS slot_rank
                    FROM {eligible}
                    WHERE fresh
                ) ranked
                JOIN {free_slots}
                    ON {free_slots}.entrypoint = ranked.entrypoint
                    AND {free_slots}.slot_rank = ranked.slot_rank
            """,
            comment="Pair each fresh claim with a free slot by rank; unpaired ones drop.",
        )

    def claim(self, eligible: Cte) -> Cte:
        """Columns: every job column."""
        return Cte(
            "claimed",
            f"""
                UPDATE {self.queue_table}
                SET status = 'picked',
                    updated   = NOW(),
                    heartbeat = NOW(),
                    queue_manager_id = {self.binds.queue_manager}
                WHERE id IN (SELECT id FROM {eligible})
                RETURNING *
            """,
            comment="Claim every eligible job in one atomic UPDATE.",
        )

    def slotted_claim(self, eligible: Cte, assignments: Cte, params: Cte) -> Cte:
        """Columns: every job column; a fresh claim left without a slot is not claimed."""
        return Cte(
            "claimed",
            f"""
                UPDATE {self.queue_table} job
                SET status = 'picked',
                    updated   = NOW(),
                    heartbeat = NOW(),
                    queue_manager_id = {self.binds.queue_manager},
                    slot = CASE WHEN {eligible}.fresh THEN {assignments}.slot ELSE job.slot END
                FROM {eligible}
                LEFT JOIN {assignments} ON {assignments}.id = {eligible}.id
                LEFT JOIN {params} ON {params}.entrypoint = {eligible}.entrypoint
                WHERE job.id = {eligible}.id
                  AND (NOT {eligible}.fresh
                       OR {params}.concurrency_limit <= 0
                       OR {assignments}.slot IS NOT NULL)
                RETURNING job.*
            """,
            comment="Claim every eligible job in one atomic UPDATE.",
        )

    def log_pick(self, claimed: Cte) -> Cte:
        return Cte(
            "log_pick",
            f"""
                INSERT INTO {self.qualified.queue_table_log} (job_id, status, entrypoint, priority)
                SELECT id, status, entrypoint, priority FROM {claimed}
            """,
            comment="Record the pick in the log table.",
        )

    @staticmethod
    def select_claimed(claimed: Cte) -> str:
        return f"SELECT * FROM {claimed} ORDER BY priority DESC, id ASC;"


def compose_dequeue(
    qualified: QualifiedNames,
    cache: dict[DequeueShape, str],
    *,
    batch_size: int,
    entrypoints: list[QueueEntrypoint],
    concurrency_limits: list[int],
    queue_manager_id: QueueManagerId,
    global_concurrency_limit: int | None,
    heartbeat_timeout: timedelta,
) -> ComposedQuery:
    """Compose the dequeue statement for the concurrency gates actually in use.

    The entrypoint gate renders only when some ``concurrency_limits`` entry is
    positive, the worker budget only when ``global_concurrency_limit`` is set;
    with neither, the statement is a plain pick-and-claim. The rendered text is
    cached per shape in ``cache`` because dequeue runs every busy-loop
    iteration; values are bound fresh on every call, so cached SQL never pins
    or shares argument objects.
    """
    if len(entrypoints) != len(concurrency_limits):
        raise ValueError("entrypoints and concurrency_limits must be the same length")
    # batch_size is rendered into the SQL text, so the annotation alone is not trusted.
    rendered: object = batch_size
    if not (isinstance(rendered, int) and not isinstance(rendered, bool) and rendered > 0):
        raise ValueError("batch_size must be a positive integer")

    composer = SqlComposer()
    binds = DequeueBinds.bind(
        composer,
        entrypoints=entrypoints,
        concurrency_limits=concurrency_limits,
        queue_manager_id=queue_manager_id,
        global_concurrency_limit=global_concurrency_limit,
        heartbeat_timeout=heartbeat_timeout,
    )
    gate, budget = binds.gate, binds.budget
    shape = DequeueShape(gate is not None, budget is not None, batch_size)
    if (cached_sql := cache.get(shape)) is not None:
        return ComposedQuery(sql=cached_sql, args=tuple(composer.values))

    fragments = DequeueFragments(qualified, binds, str(batch_size))
    if gate is None:
        statement = fragments.no_gates() if budget is None else fragments.global_gate(budget)
    elif budget is None:
        statement = fragments.entrypoint_gate(gate)
    else:
        statement = fragments.both_gates(gate, budget)
    for cte in statement.ctes:
        composer.add(cte)
    query = composer.render(statement.final)
    cache[shape] = query.sql
    return query
