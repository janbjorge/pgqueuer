# ADR-0026: The code that makes a connection controls its lifecycle

## Status

Proposed

## Summary

The code that makes a database connection controls its setup and teardown.
PgQueuer does not enter, exit or close a connection, pool or driver that it
receives.

## Context

Users give PgQueuer a connection, a pool or a driver, and often share it with
their application. The drivers tell the caller to close it. But
`QueueManager.run` and `SchedulerManager.run` enter the driver, so two parties
control one lifecycle.

## Decision

PgQueuer releases a resource that it borrows, such as a LISTEN connection from
a pool, when its last user stops. When PgQueuer makes a connection itself, for
example in the CLI, it closes it.

## Consequences

The application controls the shutdown order and can share a pool with
PgQueuer. Today `QueueManager.run` and `SchedulerManager.run` do not obey this
record. The creator of a custom driver must enter it if the driver requires
this.

## Alternatives considered

PgQueuer can enter and close each driver that it receives. We did not choose
it, because PgQueuer then closes a pool that the application still uses.

PgQueuer can enter the driver once and not close it. We did not choose it,
because an enter changes the state of some drivers, such as psycopg.

## Not covered by this ADR

This record does not control reconnection, pool size or the list of drivers.

## Revisit when

Examine this record again if PgQueuer starts to make connections from a DSN.

## References

[ADR-0000](ADR-0000-record-each-decision-in-one-short-adr.md) gives the rules.
`PoolListener` in `pgqueuer/adapters/drivers/asyncpg.py` releases the LISTEN
connection.
