# ADR-0024: SQL statements are assembled by the composer

## Status

Accepted (adoption is incremental; dequeue migrates first)

## Context

Statements used to be hand-assembled text with hand-numbered
placeholders, with the matching argument list maintained separately at
each call site. That holds while a statement has one shape. The
dequeue statement broke the pattern: concurrency gates come and go
with configuration, an optional fragment invalidates every placeholder
number after it, and the bind values must appear and disappear in
lockstep somewhere else in the code. Rendering every gate always and
neutralizing unused ones with sentinel values is no better: every
worker pays every feature's bookkeeping, because PostgreSQL plans the
SQL it is given, not the values bound to it.

## Decision

SQL is assembled by the composer. Statements are built from fragments;
every runtime value is coupled to its auto-numbered placeholder as it
is bound, and the text and argument list emerge as one pair. A
statement renders only the fragments its configuration needs, and one
configuration always renders identical text. "Not configured" is
expressed as absence, never as a sentinel value, and the composer
never rewrites fragment text.

Adoption is incremental and has not started yet: dequeue migrates
first, the remaining builders follow when next touched, and a
statement that gains a conditional fragment migrates at that moment.
Static, argument-free DDL migrates last, if ever.

## Consequences

### Positive consequences

- Placeholder numbering cannot drift: a placeholder exists exactly
  when its value was bound.
- Optional features cost nothing where they are unused.
- Deterministic per-shape text makes rendered snapshots the review
  surface for SQL changes.

### Negative consequences

- Two assembly styles coexist until migration completes.
- The composer is an in-house abstraction contributors must learn.
- Every composed shape needs its own snapshot and plan coverage.

## Alternatives considered

### Hand-numbered text per statement (the previous design)

Fine while every statement has one shape; collapses under conditional
fragments, as the dequeue statement demonstrated (renumbering churn,
or sentinel-neutralized gates that everyone pays for).

### A hand-written statement per shape

Rejected: the variant count doubles with each optional fragment, and
every fix must be repeated once per copy.

### An external query-builder dependency

Rejected: a heavyweight dependency for a handful of statements, and
the driver port's lowest-common-denominator contract (a backlog
record) exchanges plain SQL text.

## Not covered by this ADR

Migration order, per-statement fragment mechanics, caching, the
snapshot workflow, and the placeholder implementation. The dequeue
shapes are described in the
[dequeue composition model](../design/dequeue-composition.md).

## References

- [ADR index and backlog](README.md)
- [Dequeue composition model](../design/dequeue-composition.md)
- [Row locking & SKIP LOCKED reference](../reference/skip-locked.md)
