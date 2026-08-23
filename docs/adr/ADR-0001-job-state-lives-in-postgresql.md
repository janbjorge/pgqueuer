# ADR-0001: Job state lives in PostgreSQL

- Status: Accepted (retroactive)
- Date: 2026-08-23

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

## Alternatives considered

- Dedicated broker (Redis, RabbitMQ, SQS): higher raw throughput and
  purpose-built queue features, but new infrastructure and no
  transactional enqueue alongside business data.
- Database-agnostic SQL: portability across RDBMSes would forbid the
  PostgreSQL features the design leans on, such as row locking behavior
  and notifications. Rejected; PgQueuer is Postgres-only.

## Consequences

- Enqueue can share a transaction with business writes: the job exists
  exactly when the commit succeeded.
- The operational burden is the user's existing Postgres; backup, HA, and
  monitoring are already in place.
- The throughput ceiling is Postgres itself. Workloads beyond that need a
  dedicated broker and are out of scope.
- Every downstream decision (claiming, notifications, delivery
  guarantees) is constrained to what PostgreSQL offers.

## Not covered by this ADR

Table layout and SQL, client-library choice (ADR-0014), how workers claim
jobs (ADR-0002), what notifications carry (ADR-0003).
