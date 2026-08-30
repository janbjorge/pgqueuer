# Schema Revision Markers

The PgQueuer library and the schema it installs can drift apart. You upgrade the
package while the database is still on an older shape, or you upgrade the
database while some workers are still running older code — the normal state
during a rolling deploy.

To make that visible, every object `pgq install` creates carries a revision
marker in its PostgreSQL comment:

```sql
COMMENT ON TABLE pgqueuer IS '{"pgqueuer":{"schema_revision":1}}';
```

Markers go on tables, columns, indexes, the status enum type, the notify
function, and the trigger. They cost no extra database objects and survive
`pg_dump`. `pgq install` emits them as part of its single DDL script; `pgq
upgrade` applies each statement on its own and stamps last, so an upgrade that
fails partway leaves the objects it did create unstamped until the next run.

## What happens at startup

`QueueManager` and `SchedulerManager` each compare the objects the running code
requires against what the database actually has, in a single query:

| Situation | Result |
|---|---|
| The queue table is missing | Startup fails: run `pgq install` |
| A required object is missing | Startup fails, naming the object: run `pgq upgrade` |
| Only a performance index is missing | Warns, starts — run `pgq upgrade` |
| Everything is present | Starts |
| Everything is present but unmarked | Warns, starts — see below |
| An object is marked newer than the library | Warns, starts |

Indexes that exist only to make a query faster are advisory: the code runs
correctly without them, so a schema installed by an older release that never
gained one keeps starting. The two unique indexes are not advisory — they
arbitrate `ON CONFLICT` clauses, so enqueueing fails outright without them.
`SchedulerManager` is held only to the tables it uses, not to the queue-side
objects it never touches.

The last row is deliberate. During a rolling deploy the schema is upgraded
first and workers roll over afterwards, so a worker that finds a newer schema
keeps running. A revision bump may only add objects, never drop or narrow them,
so the objects an older worker needs are still there.

## Upgrading from a version before markers existed

Schemas installed before this feature have no markers. They keep working: every
required object is checked whether or not it carries a marker, so an unmarked
install is verified exactly as thoroughly as a marked one. You will see a
warning on startup:

```
PgQueuer schema has no revision markers on 55 object(s); it predates schema
revision 1. Run 'pgq upgrade' to record them.
```

Running `pgq upgrade` writes the markers and the warning stops. The upgrade is
idempotent and applies the markers even when no other schema change is needed.

## Inspecting the installed revision

`pgq verify --expect present` reports the resolved schema and prefix, the
revision the library expects, and the revision recorded per table:

```console
$ pgq verify --expect present
schema:           (search_path)
prefix:           (none)
library revision: 1
required objects: 62
  table 'pgqueuer' revision: 1
  table 'pgqueuer_log' revision: 1
  table 'pgqueuer_schedules' revision: 1
  table 'pgqueuer_statistics' revision: 1
All required PgQueuer database objects are present.
```

When something is missing it is named individually, which is usually enough to
tell a partial upgrade from a wrong `--schema`/`--prefix`.

## Notes

- The marker statements are part of the SQL emitted by `pgq sql install` and
  `pgq sql upgrade`, so installing through psql or a migration tool records
  them exactly as the Python CLI would.
- PgQueuer owns the comments on its own objects and overwrites them. If you keep
  your own notes, or a tool such as pg_graphql or PostGraphile keeps directives,
  on a PgQueuer table, they will be replaced on the next install or upgrade. An
  unreadable comment is treated as "no marker": it warns, it never fails.
- Job status enum values cannot carry comments, so they are outside this scheme.
  Adding a status value is a breaking change and ships in a major release.

## See also

- [ADR-0025](../adr/ADR-0025-the-schema-contract-is-a-declared-manifest.md)
  — why the schema is verified against a declaration rather than probed.
- [Schema manifest model](../design/schema-manifest.md) — the objects an
  installation declares and how they depend on each other.
