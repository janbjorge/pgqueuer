# Schema manifest model

What the library declares it needs from the database, and how a worker
turns that into a startup decision. The *why* lives in
[ADR-0025](../adr/ADR-0025-the-schema-contract-is-a-declared-manifest.md).
Sub-model of the [system design](README.md); the operator view is the
[schema revision markers reference](../reference/schema-revision.md).

## Flow

```
┌───────────────┐
│   Manifest    │ every object the running code requires,
│  (declared)   │ named for this installation's namespace
└───────┬───────┘
        │
        ▼
┌───────────────┐  one read of the catalog for the declared names
│    Catalog    │
│   (observed)  │
└───────┬───────┘
        │
        ▼
┌───────────────┐  declared minus observed, reduced to root causes
│   Verdict     │  and split by severity
└───────┬───────┘
        │
        ▼
┌───────────────┐
│ Startup gate  │ refuse, warn, or proceed
└───────────────┘
```

The manifest is derived from the installation's namespace, so one
declaration covers every prefix and schema.

## Dependency model

Every declared object names the object it hangs off. The relation is
containment, one level deep: no chain to walk, no cycle to break.

```
┌──────────────┐
│ Installation │
│  (namespace) │
└──────┬───────┘
       │ 1
       ├────────────────┬──────────────────┬─────────────┐
       ▼ 0..*           ▼ 0..*             ▼ 1           ▼ 1
┌─────────────┐   ┌───────────┐      ┌──────────┐  ┌──────────┐
│    Table    │   │  Routine  │      │   Type   │  │ Channel  │
└──────┬──────┘   └───────────┘      └────┬─────┘  └──────────┘
       │ 1                                │ 1
       ├──────────┬──────────┐            ▼ 0..*
       ▼ 0..*     ▼ 0..*     ▼ 0..*  ┌────────────┐
┌───────────┐ ┌───────┐ ┌─────────┐  │ Enum value │
│  Column   │ │ Index │ │ Trigger │  └────────────┘
└───────────┘ └───────┘ └─────────┘
```

Two properties depend on this shape:

- An absent table takes its columns, indexes and triggers with it, so a
  verdict names the parent and drops the dependents it explains.
- Every object resolves to a table or to the type, so a manager can be
  held to the tables it uses. `SchedulerManager` does not touch the
  queue-side objects.

The manifest records no foreign keys, no ordering, and no "created by"
edges. A dependency answers one question: is this object's absence
already explained?

## Components

| Component     | Type         | Description                                          |
|---------------|--------------|------------------------------------------------------|
| Manifest      | Value Object | The objects the running code requires                |
| Schema object | Value Object | One declared object: kind, name, parent, revision, severity |
| Observation   | Value Object | What the catalog holds for a declared name, with its recorded revision |
| Verdict       | Value Object | Not installed, incomplete, or usable, with what is absent, unstamped, or ahead |
| Startup gate  | Service      | Turns a verdict into refuse, warn, or proceed for one manager |

Manifest and verdict are pure domain values; reading the catalog belongs
to the persistence adapter.

## Severity

Severity is a property of the object, not of the situation.

| Severity   | Meaning                        | Absent object   |
|------------|--------------------------------|-----------------|
| `required` | Queries do not work without it | Refuse to start |
| `advisory` | Only makes queries faster      | Warn and start  |

A released install and the current manifest can differ by a performance
index, and a schema that worked before a library upgrade must keep
working. Uniqueness is never advisory: an index that arbitrates a
conflict clause decides how the statement behaves.

## Revision

Each declared object carries the revision its current shape dates from,
and the installed object records the revision that created it. Comparing
the two shows which side is ahead without a version table (ADR-0016).
During a rolling deploy the schema is upgraded first, so a worker that
finds newer objects reports them and keeps running.

A revision is per object and immutable once shipped. A bump may add
objects or widen types; dropping or narrowing one is a major release.

A revision records which library created an object. It is not evidence
that the object still has that shape: editing an object in place does
not change its recorded revision.

## Limits

Objects are matched by kind, name and parent. A dropped object is
therefore caught, but an altered one is not: a column retyped, a
constraint dropped, an index recreated over different columns under the
same name, or a replaced function body all satisfy the manifest.
Detecting those needs the declaration to carry each object's shape,
which it does not today.

## Invariants

- The manifest is the only place that decides whether an object is
  required.
- Existence is checked independently of the recorded revision, so an
  unstamped schema is verified as thoroughly as a stamped one.
- A verdict names only root causes.
- Whatever the verdict demands, the upgrade path must be able to create.
  Otherwise the object is advisory.
- An object the code no longer needs leaves the manifest only in a major
  release.
