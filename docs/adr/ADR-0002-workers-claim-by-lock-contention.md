# ADR-0002: Workers claim jobs by row-level lock contention, not assignment

## Status

Accepted (retroactive: documents existing behavior)

## Context

With job state in PostgreSQL (ADR-0001), multiple workers must divide the
queued rows among themselves without running the same job twice. Two
families of designs exist: a coordinator that assigns work to named
workers through partitions, shards, or leases, and contention, where
every worker races for eligible rows and the database's locking
arbitrates.

## Decision

Any worker may claim any eligible job by winning a row-level lock. The
claim query selects ready rows with `FOR UPDATE SKIP LOCKED`, so
concurrent workers skip rows already locked by others instead of
blocking or double-claiming. There is no coordinator process, no worker
registry, and no assignment of partitions or shards to particular
workers.

## Consequences

### Positive consequences

- Workers are homogeneous and stateless; adding or removing one
  involves no registration step.
- There is no coordinator to operate, and none to lose to a crash.
- PostgreSQL's row locking arbitrates every claim, so correctness does
  not depend on workers cooperating with each other.

### Negative consequences

- No fairness guarantee between workers: a fast worker close to the
  database may win a disproportionate share of jobs.
- Claiming concentrates lock traffic on the head of the queue; every
  claim is a database round-trip under contention.
- Job-to-worker affinity (routing a job to one specific machine) is not
  expressible in the claim model; entrypoint naming is the workaround.

## Alternatives considered

### Coordinator assigning work to named workers

Partitioned or leased assignment gives fairness and affinity, but it
requires worker identity, registration, and a rebalance protocol, plus
something to run them. Rejected: PgQueuer ships no service of its own
(ADR-0001), and the coordination machinery outweighs the problem at the
scale a Postgres-backed queue serves.

### Blocking `FOR UPDATE` without `SKIP LOCKED`

Workers would queue on the same head rows and serialize. Rejected:
throughput collapses as soon as a second worker starts.

### Advisory locks per job

The same contention model, but ownership would live in session state and
vanish on disconnect or through a pooler. Rejected in favor of locking
the job rows themselves; liveness is tracked as data instead
(ADR-0005).

## Not covered by this ADR

The claim query's shape (single-statement CTE, batching, priority
ordering), how concurrency limits gate claiming (ADR-0006), how stale
jobs are detected and re-claimed (ADR-0005).

## References

- [ADR index and backlog](README.md)
- [Row Locking & SKIP LOCKED](../reference/skip-locked.md)
