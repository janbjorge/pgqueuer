# Dequeue composition model

This document models how the claim statement is assembled from the
concurrency gates in use. The dequeue statement is the first adopter of
the composer policy. This document describes *what is*; the *why* lives
in [ADR-0024](../adr/ADR-0024-sql-is-assembled-by-the-composer.md).
It is a sub-model of the [system design](README.md); the participants
and the claim-time invariants named there apply unchanged.

## Flow

```
QueueManager.run / fetch_jobs (core/qm.py)
      │ batch size, entrypoint limits, worker budget (None = unlimited)
      ▼
Queries.dequeue (adapters/persistence/queries.py)
      │
      ▼
QueryQueueBuilder.build_dequeue_query (adapters/persistence/qb.py)
      │ gate predicates:
      │   capacity-gated = any per-entrypoint limit > 0
      │   budget-gated   = worker budget is not None
      ▼
SqlComposer (adapters/persistence/composer.py)
      │ bind() every runtime value → $N; cte() per required fragment
      ▼
ComposedQuery(sql, args) ──► Driver.fetch(sql, *args)
```

The two gate predicates select one of four shapes. SQL text depends
only on the shape and the installation's table names; every runtime
value travels as a bound argument.

## Shapes

| Shape           | Capacity gate | Budget gate | Extra CTEs                  | Snapshot                     |
|-----------------|---------------|-------------|-----------------------------|------------------------------|
| No gates        | off           | off         | —                           | `dequeue_no_gates.sql`       |
| Entrypoint gate | on            | off         | `params, picked, available` | `dequeue_entrypoint_gate.sql`|
| Global gate     | off           | on          | `worker_load`               | `dequeue_global_gate.sql`    |
| Both gates      | on            | on          | all of the above            | `dequeue_both_gates.sql`     |

Snapshots live in `test/query_shapes/` and are byte-compared;
regenerate an intended change with `PGQUEUER_UPDATE_SNAPSHOTS=1`.

## Bind order

Fixed per shape; the composer numbers placeholders in bind order, so a
fragment that is left out never leaves a gap.

| Placeholder | Value                          | Present         |
|-------------|--------------------------------|-----------------|
| `$1`        | batch size                     | always          |
| `$2`        | entrypoint names               | always          |
| `$3`        | queue manager id               | always          |
| `$4`        | heartbeat timeout              | always          |
| next        | per-entrypoint limits          | capacity-gated  |
| next        | worker budget                  | budget-gated    |

## Components

| Component         | Type         | Description                                            |
|-------------------|--------------|--------------------------------------------------------|
| SqlComposer       | Service      | Chains CTE fragments, auto-numbers binds; embeds bodies verbatim (`adapters/persistence/composer.py`) |
| ComposedQuery     | Value Object | Immutable pairing of rendered SQL and its argument tuple |
| QueryQueueBuilder | Service      | Selects gates, composes the shape, caches rendered text per shape (`adapters/persistence/qb.py`) |
| Gate shape        | Value Object | The `(capacity-gated, budget-gated)` pair; cache key and test axis |

## Invariants

- Unlimited is `None` at every boundary. `QueueManager.run` normalizes
  its legacy `0` to `None`; no sentinel integer reaches the builder.
- SQL text is a function of the gate shape and table names only.
  Rendered text is cached per shape on the builder instance; arguments
  are re-bound on every call and never shared between calls.
- Entrypoint names and their limits are parallel lists of equal length;
  the builder rejects a mismatch.
- Every `$N` placeholder maps onto exactly one bound argument, with no
  gaps and no extras (asserted per shape in `test/test_dequeue_shapes.py`).
- The stale-job re-pick deliberately skips the capacity gate:
  re-picking transfers ownership of an already-counted row, and gating
  it would deadlock recovery (comment in the `next_stale` CTE).

## Guards

- `test/test_composer.py` covers composer behavior: bind numbering,
  verbatim bodies, comment rendering, immutability.
- `test/test_dequeue_shapes.py` covers the snapshot per shape,
  placeholder accounting, per-shape caching, argument isolation, and
  gate semantics against a real database.
- `test/test_query_plan_regression.py` holds the EXPLAIN guards: every
  shape stays on the entrypoint-leading indexes and scans
  O(entrypoints × batch) rows.
