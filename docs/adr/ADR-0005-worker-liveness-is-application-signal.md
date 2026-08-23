# ADR-0005: Worker liveness is an application-level signal, not DB session state

## Status

Accepted (retroactive: documents existing behavior)

## Context

At-least-once delivery (ADR-0004) promises that a job owned by a dead
worker reaches another worker. Something has to define "dead". The
database offers a tempting shortcut: tie ownership to the session, for
example through advisory locks, so a lost connection releases the jobs
by itself. But connections are the least stable part of the stack.
Poolers swap them, networks drop them, and a worker that reconnects
after a blip is still alive and still mid-job.

## Decision

Workers prove liveness in data. Each running job carries a heartbeat
timestamp that its worker refreshes periodically; a job whose heartbeat
is older than a configured timeout counts as abandoned and becomes
claimable again through the ordinary claim path (ADR-0002). Job
ownership survives disconnects, reconnects, and pooler churn, because
nothing about it lives in the DB session.

## Consequences

### Positive consequences

- Ownership survives connection churn: a worker behind a pooler or a
  flaky network keeps its jobs as long as it keeps heartbeating.
- Liveness is queryable. An operator can inspect heartbeat age per job
  with plain SQL instead of decoding lock tables.
- Recovery needs no extra moving parts; stale detection is one
  predicate inside the claim query that every worker already runs.

### Negative consequences

- Recovery latency is bounded by the timeout, not instant. A crashed
  worker's jobs wait out the stale window before re-delivery.
- A worker that stalls without dying, for example on a blocked event
  loop, misses heartbeats and gets its job re-delivered while the
  original run may still finish. This is the duplicate-run case
  ADR-0004 already requires entrypoints to tolerate.
- Heartbeats cost writes: every running job refreshes its row
  periodically for as long as it runs.

## Alternatives considered

### Session-scoped locks or connection state

Advisory locks or `FOR UPDATE` locks held for the duration of the run
release the instant the connection dies, which makes detection free and
immediate. Rejected: the same mechanism releases jobs on every pooler
swap and network blip, turning routine connection churn into spurious
re-deliveries, and lock state is awkward for operators to inspect.

### External liveness service

A membership registry (etcd-style leases, or a sidecar that watches
workers) can separate "process alive" from "connection alive".
Rejected: PgQueuer introduces no service beyond the user's Postgres
(ADR-0001).

### No liveness signal, manual requeue

Abandoned jobs stay picked until an operator intervenes. Rejected: a
crash at 3 a.m. should not need a human before the queue drains again.

## Not covered by this ADR

Heartbeat intervals and timeout defaults, batching of heartbeat
updates, the index that makes stale lookup cheap, and how schedules
reuse the heartbeat idea (a backlogged record covers the scheduler).

## References

- [ADR index and backlog](README.md)
- [Heartbeat monitoring guide](../guides/heartbeat.md)
