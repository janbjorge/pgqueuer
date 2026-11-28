# ADR-0027: SQL statements are fixed text

## Status

Proposed

## Summary

For the SQL that PgQueuer runs, we write each statement as fixed text with
fixed `$n` placeholders, next to the code that runs it. Today the dequeue
statement needs 677 lines and 9 classes for its assembly (ADR-0024). A
statement with variants comes from one plain function for each variant family.
We remove the generic composer. The SQL is then easy to find and to read. The
cost is `$n` numbers that we write by hand. Also, `Queries.log_statistics`
is slower when the call gives no window.

## Context

ADR-0024 assembles statements from fragments. It rejected a hand-written
statement for each shape, because the number of variants doubles with each
optional fragment. Today the dequeue has four variants. `dequeue.py` and
`composer.py` in `pgqueuer/adapters/persistence/` assemble them in 677
lines and 9 classes. A prototype with two plain functions renders the same 84
texts in 342 lines, with no classes. Also, 53 of the 62 driver calls in
`queries.py` take their SQL from a builder in another file.

## Decision

A statement is fixed text with fixed `$n` placeholders, next to the code that
runs it. A statement with variants comes from one plain function for each
variant family. The names of database objects come from `DBSettings`, so one
configuration always gives the same text. PgQueuer has no generic composer.

## Consequences

Each statement is in one place, and a reviewer reads the SQL that PostgreSQL
receives. But we write the `$n` numbers by hand, so a change can put them out
of step with the arguments. The four dequeue texts stay until the worker budget
leaves the SQL. `Queries.log_statistics` gets one fixed text for every filter.
When the call gives no window, its median call time over 300 calls increased by
26.3% and by 36.1% in two single runs. With a window, it decreased by 2.6% and
by 4.5%.

## Alternatives considered

We can keep the composer of ADR-0024. We did not choose it, because two plain
functions give the same texts with less code and no classes.

We can use one dequeue statement for all four variants, with neutral values for
the gates that are not configured. We did not choose it, because it decreased
the jobs per second by 31.1% with 1 entrypoint and by 8.6% with 20 entrypoints.

## Not covered by this ADR

This record does not control the DDL that the schema engine renders (ADR-0016,
ADR-0025). It does not set which statements get variants and which get one text
with neutral values. It also does not control the order in which the builders
move, the snapshot tests or the driver port.

## Revisit when

Examine this record again if a statement needs more than four variants, the
number that the dequeue has today.

## References

[ADR-0000](ADR-0000-record-each-decision-in-one-short-adr.md) gives the rules.
This record supersedes [ADR-0024](ADR-0024-sql-is-assembled-by-the-composer.md).
The query builders are in `pgqueuer/adapters/persistence/qb.py`, and the
dequeue text is in `pgqueuer/adapters/persistence/dequeue.py`.
