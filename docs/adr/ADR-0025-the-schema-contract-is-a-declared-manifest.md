# ADR-0025: The schema contract is a declared manifest

## Status

Accepted

## Context

The library owns its schema lifecycle (ADR-0015) and migrates it as an
idempotent, forward-only stream with no version table (ADR-0016). It had
no stated answer to the other half: whether the schema a worker finds is
one this code can drive.

That was decided by a handful of probes, each added when an upgrade
broke a deployment. The set recorded past incidents, not present
requirements. It could not tell an old schema from a damaged one, went
stale whenever DDL was added without a matching probe, and each manager
checked a different subset.

## Decision

The database objects the library requires are declared, and startup
verifies the installed schema against that declaration.

The declaration is the only place that answers whether an object is
required. Anything it demands must have a supported way to produce;
anything optional is declared optional. Adding an object to the schema
means adding it to the declaration in the same change. Removing one is a
break in the public surface (ADR-0020).

## Consequences

### Positive consequences

- Drift is reported as the objects that differ, not as the first query
  to fail.
- One artifact to review when the schema changes, checkable against the
  schema the library installs.
- Managers can be held to the objects they use.
- Per-object versions let schema and library be compared without a
  version table.

### Negative consequences

- The declaration and the DDL can drift; only tests keep them together.
- A stricter gate can refuse a schema a previous release started on, so
  the declaration must separate required objects from ones that only
  help.
- Requiring an object commits the upgrade path to creating it.
- The declaration names objects, so it catches a dropped one and misses
  an altered one. Covering that means declaring each object's shape.

## Alternatives considered

### Probing for the shapes the code uses (the previous design)

Rejected. The probe set records past incidents, goes stale silently, and
cannot distinguish an old schema from a damaged one.

### Recording a single schema version and comparing numbers

Rejected. It reintroduces the bookkeeping ADR-0016 avoids, is
trustworthy only if nothing touches the schema out of band, and names no
object.

### Verifying nothing and letting queries fail

Rejected. The failure arrives as a driver error after the worker has
taken jobs, and carries no remedy.

## Not covered by this ADR

How the declaration is expressed, how the installed schema is read, how
per-object versions are recorded, the severity vocabulary, and which
objects are declared today. See the
[schema manifest model](../design/schema-manifest.md).

## References

- [ADR index and backlog](README.md)
- [Schema manifest model](../design/schema-manifest.md)
- [Schema revision markers reference](../reference/schema-revision.md)
