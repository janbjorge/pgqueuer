# ADR-0016: Schema upgrades are computed from the live database

## Status

Accepted

## Context

The library owns its schema lifecycle and ships install and upgrade as
library operations. It described that schema twice: one builder rendered
the target state as a create script, a second yielded an append-only
stream of idempotent statements meant to reach the same state from any
earlier release. Nothing compared them, and they had already diverged.

An index on heartbeat was created by upgrade and never by install. The
log table was created unlogged whatever the configured durability, which
install respected. The trigger function body existed verbatim in both. A
statistics column dropped from install after 0.18 had no step to remove
it, so a database from that era fails on every statistics insert.

Every schema change needed two edits held together by memory, and the
stream only grew. With no record of where a database stood, every
statement had to be idempotent and every statement ran every time.

Two constraints narrow the options. PgQueuer writes nothing to the
database beyond its working objects, so an upgrade cannot ask a database
which release it is on. A connection is available whenever an upgrade is
planned, so it can ask the catalog what that database actually has.

## Decision

One declarative model is the source of truth for the target schema.
Install renders it; upgrade applies its difference from the catalog.

The model stores each definition in the spelling PostgreSQL itself
reports, so comparing a declared object against an installed one is
string equality rather than SQL parsing, and the same string renders the
install script.

A maintainer changing the schema edits the model. Removing an object
means naming it in a separate retirement list; nothing is dropped for
being merely absent. The planner never touches an object PgQueuer does
not name, reports a retired column instead of dropping it, and raises
rather than guessing at a change it does not recognise.

Planning needs a connection; rendering does not. An offline script is
still produced from the same model for operators who apply DDL
themselves, converging absent objects without the catalog knowledge
needed to retype or redefine one.

## Consequences

### Positive consequences

- Install and upgrade cannot disagree, because they are one artifact,
  and the statement list stops growing with each release.
- An upgrade is the delta for that database, so a database from any
  earlier release converges, including objects no step ever cleaned up.
- A non-canonical spelling in the model fails a test whose diff carries
  the spelling that replaces it.

### Negative consequences

- The exact upgrade requires a connection, and the offline script is a
  weaker superset that has to be documented as one.
- Canonical spellings are a PostgreSQL version dependency, held down by
  CI across every supported major.
- An unrecognised column type change stops the upgrade and asks for an
  operator, where the previous design would have proceeded.
- An object hand-modified under a name PgQueuer owns is restored to the
  declared definition.
- Two concurrent upgrades can plan against the same stale state. An
  advisory lock narrows the window, but the statements run outside a
  single transaction, so on a pooled driver it is not a guarantee.

## Alternatives considered

### Two hand-written paths (the previous design)

Rejected. Nothing holds a snapshot and a statement stream together, and
they had already drifted in four places. The cost grows with every
release and the stream can never shrink.

### One model rendering both a create script and a converge script

Rejected as the whole answer. It removes the duplication but leaves the
upgrade touching every object on every run, and retypes and index
redefinitions stay hand-maintained. Retained for the offline case, where
there is no catalog to read.

### Versioned migrations with a recorded schema version

Rejected. It reintroduces the bookkeeping this project declines to
carry, and a recorded version is trustworthy only if nothing touches the
schema out of band.

### Vendoring a schema-diff tool

Rejected. migra pulls in SQLAlchemy and is barely maintained;
pg-schema-diff and Atlas are Go binaries. A library whose schema is four
tables, a function and a trigger should not acquire a runtime dependency
to describe it.

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
