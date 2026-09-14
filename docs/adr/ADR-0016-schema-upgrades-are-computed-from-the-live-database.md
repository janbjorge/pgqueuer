# ADR-0016: Schema upgrades are computed from the live database

## Status

Accepted

## Context

PgQueuer owns its schema lifecycle and ships install and upgrade as
library operations. It described that schema twice: a create script for
the target state, and an append-only stream of idempotent statements
meant to reach it from any earlier release. Nothing compared the two,
and they had already diverged. Upgrade created an index install never
created, created the log table unlogged whatever durability was
configured, and duplicated the trigger function body verbatim. A
statistics column dropped from install after 0.18 had no step to remove
it, so a database from that era fails on every statistics insert.

Every change needed two edits held together by memory, and with no
record of where a database stood, every statement had to be idempotent
and ran every time. Two constraints shape the fix: PgQueuer writes
nothing beyond its working objects, so it cannot ask which release a
database is on, but a connection is available whenever an upgrade is
planned, so it can ask the catalog.

## Decision

One declarative model is the source of truth for the target schema.
Install renders that model. Upgrade reads the catalog and applies
whatever differs.

The model stores each definition in the spelling PostgreSQL itself
reports, so comparing a declared object against an installed one is
string equality rather than SQL parsing, and the same string renders the
install script.

A maintainer changing the schema edits the model. Removing an object
means naming it in a separate retirement list, because nothing is
dropped for being merely absent. The planner never touches an object
PgQueuer does not name, reports a retired column rather than dropping
it, and raises on a change it does not recognise.

Rendering needs no connection, so an offline script is still produced
for operators who apply DDL themselves. It converges absent objects but
cannot retype or redefine one.

## Consequences

### Positive consequences

- Install and upgrade cannot disagree, because they are one artifact,
  and the statement list stops growing with each release.
- An upgrade is the delta for that database, so one from any earlier
  release converges, including objects no step ever cleaned up.
- A non-canonical spelling fails a test that shows the spelling to use.

### Negative consequences

- The exact upgrade needs a connection, and the offline script is a
  weaker superset that has to be documented as one.
- PostgreSQL can change a canonical spelling between major versions, so
  CI has to check every supported major.
- An unrecognised column type change stops the upgrade and waits for a
  human, where the previous design would have proceeded.
- An object hand-modified under a name PgQueuer owns is reverted to the
  declared definition.
- Two concurrent upgrades can plan against the same stale state. An
  advisory lock narrows the window, but the statements run outside a
  single transaction, so on a pooled driver it is not a guarantee.

## Alternatives considered

### Two hand-written paths (the previous design)

Rejected. Nothing holds a snapshot and a statement stream together, they
had already drifted in four places, and the stream can never shrink.

### One model rendering both a create script and a converge script

Rejected as the whole answer. The upgrade still touches every object on
every run and retypes stay hand-maintained. Retained for the offline
case, where there is no catalog to read.

### Versioned migrations with a recorded schema version

Rejected. It reintroduces the bookkeeping this project declines to
carry, and the recorded version is trustworthy only if nothing touches
the schema out of band.

### Vendoring a schema-diff tool

Rejected. migra pulls in SQLAlchemy and is barely maintained;
pg-schema-diff and Atlas are Go binaries. A library whose schema is four
tables, a function and a trigger should not take a dependency to
describe it.

## Not covered by this ADR

The model's field layout, the catalog queries, the statement ordering,
the CLI flag spellings, and how the model relates to the declared
manifest of ADR-0025. See the
[schema manifest model](../design/schema-manifest.md).

## References

- [ADR index and backlog](README.md)
- [Schema manifest model](../design/schema-manifest.md)
- [Database setup](../reference/database-setup.md)
- [CLI reference](../reference/cli.md)
