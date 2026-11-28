# ADR-0028: Persistence internals can change in a minor release

## Status

Proposed

## Summary

"Versioning" in `AGENTS.md` includes import paths and class fields in the
public API. Because of this, the internals in `pgqueuer/adapters/persistence/`
can change only in a major release. We let a minor release change these
persistence internals, with a release note. We do not keep each importable
name public until v3. The persistence code can then become smaller before
v3. But user code that imports the persistence internals can break in a
minor release.

## Context

"Versioning" has no rule for internal code. But the project already calls
persistence code internal in two places. The upgrade guide to v1.0 calls
`pgqueuer.qb` "internal-only". The minor release v1.4.0 changed two builder
signatures, with the release note "Internal, but reachable via
`Queries.qbq`". The planned SQL simplification removes more of this code,
for example `pgqueuer/adapters/persistence/composer.py`. Under "Versioning",
it must wait for v3.

## Decision

The persistence internals are the code in `pgqueuer/adapters/persistence/`:
module names, import paths, classes, fields, methods and functions. They
include the query builders in `qb.py`, also when a field of a public class
holds one, such as `Queries.qbq`. A minor release can change the
persistence internals. Its release note must state each change.

Names that `pgqueuer/__init__.py` or a root shim exports stay public, also
when their code is in `pgqueuer/adapters/persistence/`. Examples are
`Queries` and `SyncQueries` in `pgqueuer/queries.py`. Their fields stay
public, but the builders in `qbe`, `qbq` and `qbs` can change.

## Consequences

The SQL simplification can go into minor releases of v2. User code that
imports the persistence internals can break in a minor release. Such user
code is probable, because the upgrade guide to v1.0 tells users of
`pgqueuer.qb` to use `pgqueuer.adapters.persistence.qb`.

## Alternatives considered

Each importable name can stay public until a major release, as "Versioning"
says today. We did not choose it, because the SQL simplification then waits
for v3.

PgQueuer can keep the old code with a deprecation warning until v3. We did
not choose it, because the old code and the new code then both stay until
v3.

## Not covered by this ADR

This record does not control the database schema, the CLI, the protocols in
`pgqueuer/ports/` or the other adapters. For example, `Queries.qbe.settings`
stays, because `QueryBuilderEnvironmentPort` in
`pgqueuer/ports/repository.py` declares it.

## Revisit when

Examine this record again when the record on strict SemVer merges. That
record must define the public API. Then put this rule in that record, and
set the status of this record to `Superseded by ADR-NNNN`.

## References

[ADR-0000](ADR-0000-record-each-decision-in-one-short-adr.md) gives the rules.
The v1.4.0 section of `RELEASE.md` has the note on the builders. Section 8
of `docs/getting-started/upgrading.md` lists the internal modules of v1.0.
