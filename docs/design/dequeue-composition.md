# Dequeue composition model

How the claim statement is assembled from the concurrency gates in
use. This document describes *what is*; the *why* lives in
[ADR-0024](../adr/ADR-0024-sql-is-assembled-by-the-composer.md). It is
a sub-model of the [system design](README.md).

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

Fixed per shape; a fragment that is left out never leaves a gap.

| Placeholder | Value                          | Present         |
|-------------|--------------------------------|-----------------|
| `$1`        | batch size                     | always          |
| `$2`        | entrypoint names               | always          |
| `$3`        | queue manager id               | always          |
| `$4`        | heartbeat timeout              | always          |
| next        | per-entrypoint limits          | capacity-gated  |
| next        | worker budget                  | budget-gated    |

## Components

| Component         | Type         | Description                                       |
|-------------------|--------------|---------------------------------------------------|
| SqlComposer       | Service      | Chains CTE fragments, auto-numbers binds, embeds bodies verbatim |
| ComposedQuery     | Value Object | Immutable pairing of rendered SQL and its arguments |
| QueryQueueBuilder | Service      | Selects gates, composes the shape, caches text per shape |
| Gate shape        | Value Object | The `(capacity-gated, budget-gated)` pair; cache key and test axis |

## Invariants

- Unlimited is `None` at every boundary; `QueueManager.run` normalizes
  its legacy `0` to `None`.
- Rendered text is cached per shape on the builder instance; arguments
  are re-bound on every call and never shared between calls.
- Entrypoint names and limits are parallel lists of equal length; the
  builder rejects a mismatch.
- Every `$N` maps onto exactly one bound argument.
- The stale-job re-pick skips the capacity gate on purpose: re-picking
  transfers ownership of an already-counted row, and gating it would
  deadlock recovery.

## Guards

- `test/test_composer.py`: composer behavior.
- `test/test_dequeue_shapes.py`: snapshots, placeholder accounting,
  caching, argument isolation, gate semantics.
- `test/test_query_plan_regression.py`: every shape stays on the
  entrypoint-leading indexes and scans O(entrypoints × batch) rows.
