# ADR-0024: SQL statements are assembled by the composer

## Status

Accepted (adoption is incremental; the dequeue statement is the first
adopter)

## Context

Every statement the library sends used to be built the same way: an
f-string with hand-numbered `$N` placeholders in one place, and the
matching argument list assembled separately at the call site. That
holds exactly as long as each statement has one shape. The dequeue
statement broke the pattern: concurrency gates come and go with
configuration, an optional fragment invalidates every hand-written
placeholder number after it, and the bind values must appear and
disappear in lockstep somewhere else in the code. Text and argument
tuple are two facts that must never drift, maintained by hand in two
places.

The alternative failure mode is just as costly: rendering every
optional fragment always and neutralizing unused ones with sentinel
values makes every worker pay every feature's bookkeeping, because
PostgreSQL plans the SQL it is given, not the values bound to it.

## Decision

SQL is assembled by the composer. A statement is built from fragments;
every runtime value is coupled to its auto-numbered placeholder at the
moment it is bound; the rendered text and the argument tuple emerge as
one pair. Statements render only the fragments their configuration
needs, and one configuration always renders byte-identical text, so
each reachable shape is a reviewable, snapshot-testable SQL artifact.

Two corollaries:

- "Not configured" is expressed as absence (`None`) at every boundary,
  never as a sentinel value — a sentinel reads as configuration and
  silently re-enables the fragment it means to disable.
- The composer stays transparent: fragment bodies are embedded
  verbatim, never rewritten, so what the author wrote is what Postgres
  receives.

Adoption is incremental. The dequeue statement is composed today; the
remaining builders migrate when they are next touched, and any
statement that gains a conditional fragment migrates at that moment
rather than growing a second hand-numbered variant. Static,
argument-free DDL carries no numbering risk and migrates last, if
ever.

## Consequences

### Positive consequences

- Placeholder numbering cannot drift: a `$N` exists if and only if its
  value was bound, however many optional fragments surround it.
- Optional features cost nothing where they are unused; workers without
  concurrency gates run gate-free SQL.
- Deterministic per-shape text makes snapshots the review surface for
  SQL changes and keeps driver-side prepared-statement caches at one
  entry per shape.

### Negative consequences

- Two assembly styles coexist until migration completes; a reader of
  `qb.py` meets both.
- The composer is an in-house abstraction contributors must learn, and
  it must be held to the transparency rule above or it becomes its own
  source of SQL bugs.
- Every composed shape needs its own snapshot and plan coverage; a
  shape production cannot reach can hide an untested one it can.

## Alternatives considered

### Hand-numbered f-strings per statement (the previous design)

Fine while every statement has one shape; collapses under conditional
fragments, where the dequeue statement demonstrated both failure modes
(renumbering churn, or sentinel-neutralized gates that everyone pays
for).

### A hand-written statement per shape

Static SQL files, one per configuration combination. Rejected: the
variant count doubles with each optional fragment and the shared
skeleton is duplicated into every copy, so a fix must be repeated once
per file.

### An external query-builder dependency

An expression-tree builder (SQLAlchemy Core or similar) composes
conditional SQL natively. Rejected: a heavyweight dependency for a
handful of statements, the driver port's lowest-common-denominator
contract (a backlog record) exchanges plain SQL text, and the
library's SQL is deliberately reviewable as SQL.

## Not covered by this ADR

The migration order of the remaining builders (maintainer judgement,
statement by statement), the CTE mechanics of any one statement, the
caching of rendered text, the snapshot-update workflow, and the
placeholder implementation. The dequeue statement's shapes and
invariants are described in the
[dequeue composition model](../design/dequeue-composition.md).

## References

- [ADR index and backlog](README.md)
- [Dequeue composition model](../design/dequeue-composition.md)
- [Row locking & SKIP LOCKED reference](../reference/skip-locked.md)
