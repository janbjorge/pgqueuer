# ADR-0001: Job state lives in PostgreSQL

## Status

Accepted (retroactive: documents existing behavior)

## Context

A job queue needs a home for its state: queued jobs, running jobs,
outcomes. The common answer is a dedicated broker such as Redis, RabbitMQ,
or SQS. A broker is one more service to deploy, monitor, back up, and
secure, and a job enqueued to it can never join the application's database
transaction.

## Decision

The queue's source of truth is a PostgreSQL database the user already
operates. PgQueuer is a library plus a schema. It introduces no broker
process and no service of its own.

## Consequences

### Positive consequences

- Enqueue can share a transaction with business writes: the job exists
  exactly when the commit succeeded.
- The operational burden is the user's existing Postgres; backup, HA, and
  monitoring are already in place.
- One less system to secure and keep on-call for.

### Negative consequences

- The throughput ceiling is Postgres itself. Workloads beyond that need a
  dedicated broker and are out of scope.
- Queue traffic and business queries compete for the same database
  resources.
- Every downstream decision (claiming, notifications, delivery
  guarantees) is constrained to what PostgreSQL offers.

## Alternatives considered

### Dedicated broker (Redis, RabbitMQ, SQS)

Higher raw throughput and purpose-built queue features. Rejected because
it adds new infrastructure and gives up transactional enqueue alongside
business data, which is the main reason to pick PgQueuer at all.

### Database-agnostic SQL

Portability across RDBMSes would forbid the PostgreSQL features the
design leans on, such as row locking behavior and notifications.
Rejected; PgQueuer is Postgres-only.

## Not covered by this ADR

Table layout and SQL, client-library choice (still in the backlog), how
workers claim jobs (ADR-0002), what notifications carry (ADR-0003).

## References

- [ADR index and backlog](README.md)
- [When PgQueuer fits](../getting-started/when-to-use.md)
