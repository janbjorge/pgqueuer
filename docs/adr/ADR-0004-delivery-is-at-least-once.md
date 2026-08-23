# ADR-0004: Delivery is at-least-once

## Status

Accepted (retroactive: documents existing behavior)

## Context

A worker can die at any point between claiming a job (ADR-0002) and
recording its outcome. The queue must decide what happens to that job:
give it to another worker, which risks running it twice, or refuse to,
which risks never running it. Exactly-once execution would need extra
machinery on the consumer side, such as fencing tokens or a
transactional outbox, and even then it only holds when the job's side
effects live in the same database.

## Decision

A job may run more than once; PgQueuer does not attempt exactly-once.
When a worker crashes between claim and completion, the job is
re-delivered to another worker. The user contract follows from this:
entrypoints must be idempotent, and the documentation states that
requirement wherever job execution is described.

## Consequences

### Positive consequences

- No job is lost to a worker crash; re-delivery is the recovery path.
- The queue core stays simple: no fencing tokens, no consumer-side
  transaction protocol, no coordination with the entrypoint's side
  effects.
- The contract is honest. Systems that claim exactly-once still degrade
  to at-least-once when side effects leave the database; PgQueuer names
  the guarantee users actually get.

### Negative consequences

- Idempotency becomes the user's problem, and a non-idempotent
  entrypoint fails in ways that only show up when a crash or timeout
  triggers re-delivery.
- A crash re-delivery is indistinguishable from a first delivery. The
  job's `attempts` counter tracks explicit database retries;
  re-picking a stale job does not touch it, so the duplicate run
  carries no marker.
- The documentation has to repeat the idempotency requirement loudly,
  because nothing in the API forces it.

## Alternatives considered

### Exactly-once via consumer-side machinery

Fencing tokens, a transactional outbox, or completion records checked
before each run. Rejected: the machinery is complex, it constrains what
an entrypoint may do, and the guarantee silently collapses to
at-least-once the moment a side effect happens outside the queue's
database.

### At-most-once

Never re-deliver: a claimed job that misses its completion is gone.
Rejected: silently losing jobs on a worker crash is the worse failure
mode for a job queue; users who want fire-and-forget can express that
in the entrypoint.

## Not covered by this ADR

How a crashed worker is detected (worker liveness has its own backlog
entry), what happens to jobs that fail rather than crash (the
`on_failure` disposition is a consequence of this record and of the
planned outcome-log record), retry counting and backoff (durable
retries, still in the backlog).

## References

- [ADR index and backlog](README.md)
- [Reliability model](../guides/reliability.md)
