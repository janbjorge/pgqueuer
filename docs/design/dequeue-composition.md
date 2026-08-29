# Dequeue composition model

How the claim statement is assembled from the concurrency gates in
use. This document describes *what is*; the *why* lives in
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
depends only on the shape and the installation's table names; every
runtime value travels as a bound argument.

## Shapes

| Shape           | Entrypoint limits | Worker budget |
|-----------------|-------------------|---------------|
| No gates        | off               | off           |
| Entrypoint gate | on                | off           |
| Global gate     | off               | on            |
| Both gates      | on                | on            |

## Components

| Component     | Type         | Description                                        |
|---------------|--------------|----------------------------------------------------|
| Composer      | Service      | Assembles a statement from fragments; couples each value to its placeholder as it is bound |
| Composed query| Value Object | Immutable pairing of rendered text and arguments   |
| Query builder | Service      | Derives the gate predicates and composes the shape |
| Gate shape    | Value Object | The pair of gate predicates; selects the statement |

The composer and builder live in `pgqueuer/adapters/persistence/`.

## Invariants

- "Unlimited" is expressed as absence at every boundary, never as a
  sentinel value.
- One gate configuration always renders identical text; arguments are
  bound fresh on every claim and never shared between claims.
- The stale-job re-pick skips the entrypoint gate on purpose:
  re-picking transfers ownership of an already-counted job, and gating
  it would deadlock recovery.

## Guards

Each shape's rendered text is snapshot-tested, and plan-regression
tests assert every shape stays on the entrypoint-leading indexes and
scans work proportional to entrypoints × batch, not the backlog.
