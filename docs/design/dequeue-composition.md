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
┌───────────────┐
│ Dequeue SQL   │ picks the shape, lists its CTEs
└───────┬───────┘
        │
        ▼
┌───────────────┐
│ Query template│ cached per shape and batch size
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

| Shape           | Entrypoint limits | Worker budget |
|-----------------|-------------------|---------------|
| No gates        | off               | off           |
| Entrypoint gate | on                | off           |
| Global gate     | off               | on            |
| Both gates      | on                | on            |

`dequeue_shape` picks the shape. Each shape is one method on `DequeueSql`
listing its CTEs in `WITH` order; each CTE is one method. Snapshots live
under `test/query_shapes/`.

## Components

| Component         | Type         | Description                                   |
|-------------------|--------------|-----------------------------------------------|
| Template composer | Service      | Numbers placeholders, records each one's reader |
| Query template    | Value Object | Rendered text and its readers                 |
| CTE               | Value Object | Name, body and one-line log comment           |
| Composed query    | Value Object | Rendered text paired with its arguments       |
| Dequeue args      | Value Object | One call's values, validated on construction  |
| Dequeue shape     | Value Object | Which gates the statement renders             |
| Dequeue SQL       | Service      | One method per shape and per CTE              |

Code: `pgqueuer/adapters/persistence/composer.py` and `dequeue.py`. The
builder caches one template per shape and batch size.

## Invariants

- "Unlimited" is expressed as absence at every boundary, never as a
  sentinel value.
- One gate configuration and batch size always render identical text;
  arguments are bound fresh on every claim and never shared between claims.
- Each placeholder carries its own reader, so a cached template never
  depends on bind order.
- Rendering rejects a CTE name defined twice.
- The stale-job re-pick skips the entrypoint gate on purpose:
  re-picking transfers ownership of an already-counted job, and gating
  it would deadlock recovery.

## Guards

Each shape's rendered text is snapshot-tested, and plan-regression
tests assert every shape stays on the entrypoint-leading indexes and
scans work proportional to entrypoints × batch, not the backlog.
