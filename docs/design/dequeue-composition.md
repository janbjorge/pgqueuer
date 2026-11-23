# Dequeue composition model

How the claim statement is assembled from the concurrency gates in
use. This document describes the target design; the *why* and the
adoption status live in
[ADR-0024](../adr/ADR-0024-sql-is-assembled-by-the-composer.md). It is
a sub-model of the [system design](README.md).

## Flow

```
┌───────────────┐
│ Queue manager │ batch size, entrypoint limits, worker budget
└───────┬───────┘
        │
        ▼
┌───────────────┐  derives the gate shape from what is configured
│ Query builder │  and picks the fragments that shape needs
└───────┬───────┘
        │ fragments and values, in a fixed order
        ▼
┌───────────────┐  couples each value to its placeholder as it is
│   Composer    │  bound; chains the fragments into one statement
└───────┬───────┘
        │
        ▼
┌────────────────┐
│ Composed query │ immutable pair: rendered text + arguments
└───────┬────────┘
        │
        ▼
┌───────────────┐
│    Driver     │ sends the pair to PostgreSQL
└───────────────┘
```

The two gate predicates (is any entrypoint limit configured, is a
worker budget configured) select one of four shapes. The rendered text
depends only on the shape, the batch size and the installation's table
names. The batch size is a literal, so the plan PostgreSQL caches for
the prepared statement can see its `LIMIT`; every other runtime value
travels as a bound argument.

## Shapes

| Shape           | Entrypoint limits | Worker budget | Snapshot                      |
|-----------------|-------------------|---------------|-------------------------------|
| No gates        | off               | off           | `dequeue_no_gates.sql`        |
| Entrypoint gate | on                | off           | `dequeue_entrypoint_gate.sql` |
| Global gate     | off               | on            | `dequeue_global_gate.sql`     |
| Both gates      | on                | on            | `dequeue_both_gates.sql`      |

Each shape is one method on `DequeueFragments` (`no_gates`,
`entrypoint_gate`, `global_gate`, `both_gates`) that reads as the
statement's data flow: every CTE is a method that takes the CTEs it
reads as arguments. Snapshots live under `test/query_shapes/`.

### Data flow

Without entrypoint limits, the statement claims straight off the
backlog:

```
available                ──▶ next_queued
next_queued, next_stale  ──▶ eligible ──▶ claimed ──▶ log_pick
```

With entrypoint limits, capacity is counted first, unlimited and
limited entrypoints claim separately, and every fresh claim needs a
free slot:

```
params, picked                      ──▶ available
available                           ──▶ unlimited_claims, limited_claims
unlimited_claims, limited_claims    ──▶ next_queued
next_queued, next_stale             ──▶ eligible
available, params                   ──▶ free_slots
eligible, free_slots                ──▶ slot_assignments
eligible, slot_assignments, params  ──▶ claimed ──▶ log_pick
```

The worker budget adds `worker_load` to either flow. It is read in
three places: `next_queued` and `next_stale` stop when the worker is at
its budget, and `eligible` takes at most `worker_load.headroom` rows.

### CTE matrix

| CTE                | No gates | Entrypoint gate | Global gate | Both gates |
|--------------------|----------|-----------------|-------------|------------|
| `params`           |          | ✓               |             | ✓          |
| `picked`           |          | ✓               |             | ✓          |
| `available`        | all      | gated           | all         | gated      |
| `worker_load`      |          |                 | ✓           | ✓          |
| `unlimited_claims` |          | ✓               |             | ✓          |
| `limited_claims`   |          | ✓               |             | ✓          |
| `next_queued`      | scan     | merged          | scan        | merged     |
| `next_stale`       | ✓        | + entrypoint    | ✓           | + entrypoint |
| `eligible`         | plain    | slotted         | plain       | slotted    |
| `free_slots`       |          | ✓               |             | ✓          |
| `slot_assignments` |          | ✓               |             | ✓          |
| `claimed`          | plain    | slotted         | plain       | slotted    |
| `log_pick`         | ✓        | ✓               | ✓           | ✓          |

### Why `limited_claims` is nested three deep

A locking clause under a `LIMIT` keeps skipping locked rows until the
limit is met, so it slides down the backlog past the entrypoint's cap
(#761). The window of candidate ids is therefore fixed first, capped
by `remaining`, and only then locked row by row. The innermost constant
`LIMIT` exists for the planner: it cannot evaluate the `LEAST` cap and
would otherwise cost a share of the whole backlog, in both custom and
generic plans (#1099).

## Components

| Component          | Type         | Description                                        |
|--------------------|--------------|----------------------------------------------------|
| Composer           | Service      | Couples each value to its placeholder as it is bound; chains CTEs into one statement |
| CTE                | Value Object | Name, body and log comment; formats as its name inside a later body |
| Composed query     | Value Object | Immutable pairing of rendered text and arguments   |
| Dequeue binds      | Value Object | Every placeholder of one dequeue, bound in one fixed order; an unconfigured gate is `None` |
| Dequeue fragments  | Service      | One method per CTE, one method per shape           |
| Gate shape         | Value Object | The gate predicates and the batch size; keys the rendered-text cache |

The composer lives in `pgqueuer/adapters/persistence/composer.py`, the
dequeue statement in `pgqueuer/adapters/persistence/dequeue.py`.
`QueryQueueBuilder.build_dequeue_query` delegates to `compose_dequeue`.

## Invariants

- "Unlimited" is expressed as absence at every boundary, never as a
  sentinel value.
- One gate configuration and batch size always render identical text;
  arguments are bound fresh on every claim and never shared between claims.
- Values are bound before the cache lookup, in the one order
  `DequeueBinds.bind` fixes, so cached text and fresh arguments agree
  on every `$N`.
- A CTE name is defined once per statement; shape variants of one CTE
  (`all_available` and `gated_available`) share the name, and the
  composer rejects a second definition.
- The stale-job re-pick skips the entrypoint gate on purpose:
  re-picking transfers ownership of an already-counted job, and gating
  it would deadlock recovery.

## Guards

Each shape's rendered text is snapshot-tested, and plan-regression
tests assert every shape stays on the entrypoint-leading indexes and
scans work proportional to entrypoints × batch, not the backlog.
