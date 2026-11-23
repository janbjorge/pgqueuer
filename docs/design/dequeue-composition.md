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
┌───────────────┐  dequeue_shape picks the shape from what is
│ Dequeue SQL   │  configured; the shape method lists its CTEs
└───────┬───────┘
        │ CTEs, and one reader per placeholder
        ▼
┌───────────────┐  rendered once per shape and batch size,
│ Query template│  cached, bound to the call's arguments
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

`dequeue_shape` is the one place the shape is decided. Each shape is
one method on `DequeueSql` (`no_gates`, `entrypoint_gate`,
`global_gate`, `both_gates`) that lists its CTEs in `WITH` order. Each
CTE is one method; a CTE with two forms has two methods
(`available` and `available_with_capacity`) that render under the same
name. Bodies name the CTEs they read as plain SQL, and the docstring
records the columns a CTE returns and the CTEs it reads. Snapshots
live under `test/query_shapes/`.

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
| `available`        | `available` | `available_with_capacity` | `available` | `available_with_capacity` |
| `worker_load`      |          |                 | ✓           | ✓          |
| `unlimited_claims` |          | ✓               |             | ✓          |
| `limited_claims`   |          | ✓               |             | ✓          |
| `next_queued`      | `next_queued` | `next_queued_from_claims` | `next_queued` | `next_queued_from_claims` |
| `next_stale`       | ✓        | ✓               | ✓           | ✓          |
| `eligible`         | `eligible` | `eligible_with_slots` | `eligible` | `eligible_with_slots` |
| `free_slots`       |          | ✓               |             | ✓          |
| `slot_assignments` |          | ✓               |             | ✓          |
| `claimed`          | `claimed` | `claimed_with_slots` | `claimed` | `claimed_with_slots` |
| `log_pick`         | ✓        | ✓               | ✓           | ✓          |

Where a cell names a method, that method renders the CTE in that shape.

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
| Template composer  | Service      | Numbers each placeholder and records how to read its value from a call's arguments |
| Query template     | Value Object | Rendered text and one reader per placeholder; `bind(args)` yields a composed query |
| CTE                | Value Object | Name, SQL body and single-line log comment         |
| Composed query     | Value Object | Immutable pairing of rendered text and arguments   |
| Dequeue args       | Value Object | The values one dequeue call binds                  |
| Dequeue shape      | Value Object | `Literal` of the four shapes, chosen by `dequeue_shape` |
| Dequeue SQL        | Service      | One method per shape, one method per CTE           |

The composer lives in `pgqueuer/adapters/persistence/composer.py`, the
dequeue statement in `pgqueuer/adapters/persistence/dequeue.py`.
`QueryQueueBuilder.build_dequeue_query` delegates to `compose_dequeue`,
which renders a template once per shape and batch size and binds it on
every call.

## Invariants

- "Unlimited" is expressed as absence at every boundary, never as a
  sentinel value.
- One gate configuration and batch size always render identical text;
  arguments are bound fresh on every claim and never shared between claims.
- A cached template pairs with fresh arguments because each placeholder
  carries its own reader; nothing depends on the order values are bound.
- A CTE name is defined once per statement; rendering rejects a second
  definition, so two forms of one CTE cannot both reach a shape.
- The stale-job re-pick skips the entrypoint gate on purpose:
  re-picking transfers ownership of an already-counted job, and gating
  it would deadlock recovery.

## Guards

Each shape's rendered text is snapshot-tested, and plan-regression
tests assert every shape stays on the entrypoint-leading indexes and
scans work proportional to entrypoints × batch, not the backlog.
