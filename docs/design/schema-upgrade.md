# Schema upgrade model

How an installed database is brought to the schema the running release
declares. The *why* lives in
[ADR-0016](../adr/ADR-0016-schema-upgrades-are-computed-from-the-live-database.md).
It is a sub-model of the [system design](README.md); whether a worker
may start against a database is the
[schema manifest model](schema-manifest.md).

## Flow

```
┌───────────────┐                    ┌───────────────┐
│  Declaration  │ target(settings)   │    Catalog    │ inspect(): declared,
│ domain/schema │                    │ (pg_catalog)  │ retired and rebuild
└───────┬───────┘                    └───────┬───────┘ names only
        │ Schema (declared)                  │ Schema (live)
        └─────────────────┬──────────────────┘
                          ▼
                 ┌─────────────────┐  string equality on both sides,
                 │     Planner     │  both spelled bare
                 │  plan(live, …)  │
                 └────────┬────────┘
                          │ Plan: statements + notes, or SchemaDriftError
                          ▼
                 ┌─────────────────┐  session advisory lock around
                 │     Applier     │  plan + apply; one statement
                 │  apply_upgrade  │  per round trip, no transaction
                 └────────┬────────┘
                          ▼
                 ┌─────────────────┐  statements run; notes logged
                 │    Operator     │  and printed as "note:" lines
                 └─────────────────┘
```

The declaration in `pgqueuer/domain/schema/` is the only thing a
maintainer edits. Install renders it, `pgq upgrade` diffs the catalog
against it, `pgq sql upgrade` renders an offline converge script from
it, and uninstall drops what it names. Only `pgq upgrade` diffs; `pgq
install` reads the catalog just to refuse a database that already holds
a declared table, a declared enum or a retired type.

## Comparison

Both sides are a `Schema` of frozen dataclasses, so a diff is plain
equality. That works because the declaration spells every definition
the way PostgreSQL reports it: column types as `format_type`, defaults
as `pg_get_expr`, index bodies as the `pg_get_indexdef` tail from
` USING `, function bodies as `prosrc`. Inspection strips the
`<db_schema>.` qualifier PostgreSQL adds, folds a `nextval(...)` default
into the `serial` column kind, and leaves constraint-backed indexes to
the table. Function bodies compare with whitespace collapsed. Triggers
compare on name, table and function only.

Inspection reads only the names the declaration, the retirement list and
the rebuild protocol produce, in one namespace. An object PgQueuer does
not name is never read and never touched.

## Planning

Each difference becomes a statement, a note, or a refusal. Absent
objects are created, `NOT NULL` and defaults are brought into line, and
an index defined differently is rebuilt. Two column conversions are
recognised: `integer` to `bigint` for the id, and any type to the status
enum through `::TEXT`. Both rewrite the table, so whenever one is
planned a note warns about the `ACCESS EXCLUSIVE` lock. Any other type
change raises `SchemaDriftError` while planning, before any statement
runs.

A note is something the upgrade will not do on its own. Retired columns
hold data, so they get `DROP NOT NULL` and a note, never `DROP COLUMN`.
Durability is never changed here; a persistence mismatch is a note
pointing at `pgq durability`. A narrow id column or sequence with
`widen_id` off, a serial/identity mismatch and a missing primary key
are notes too.

The planner emits statements in dependency order:

```
namespace → enums → tables → retired cleanup → indexes → routines → retired types
            │       │        │                 │
            │       │        │                 └ fold statistics buckets
            │       │        │                   before their unique index
            │       │        └ retired index drops,
            │       │          retired column DROP NOT NULL
            │       └ create, add column, retype, constraints,
            │         sequence, unique constraints
            └ labels exist before a retype uses them
```

Columns come before indexes so an index never covers a column the plan
has yet to add or retype, and retired types come last because a column
references one until its retype has run. Retired index drops sit after
column work, so widening `id` on an old database rewrites a retired
index once before dropping it.

## Applying

`apply_upgrade` plans and applies inside one session-scoped advisory
lock, keyed on `crc32` of the qualified queue table, so what runs is
exactly what was planned. Statements go one per round trip with no
surrounding transaction, because `ALTER TYPE … ADD VALUE` cannot share
one with a statement using the new label. A failure therefore leaves the
earlier statements applied, and a rerun plans what remains. That is why
every statement must be safe to be the last one that ran.

The lock binds one connection; over a pool, statements can land
elsewhere and escape it, which is why `pgq upgrade` uses a single
connection. Two installations contend only when their qualified queue
tables match, including same-named installations separated only by
`search_path`. Notes are logged after the lock is released, and the CLI
prints them as `note:` lines on stderr so `pgq upgrade --plan` keeps its
SQL alone on stdout.

## Index rebuild

A redefined index is built beside the old one under a fixed-width name,
`pgq_rebuild_<crc32>`, then swapped in:

```
                 DROP replacement IF EXISTS
                 CREATE replacement
  ┌───────────┐ ──────────────────────────► ┌──────────────────┐
  │ old only  │                             │ old + replacement│
  └───────────┘ ◄── build fails: old kept   └────────┬─────────┘
                                                     │ DROP old
                                                     ▼
  ┌───────────┐       RENAME replacement    ┌──────────────────┐
  │ new only  │ ◄────────────────────────── │ replacement only │
  └───────────┘                             └──────────────────┘
```

Inspection reads rebuild names, so a run cut off anywhere converges on
the next plan. If the old index is still there it differs, so the
rebuild starts again and its first `DROP` clears the leftover. If only
the replacement is left and it matches, it is renamed in; if it is from
an older declaration, it is dropped and the index created fresh. Until
the rename, the new index carries its rebuild name, so a slot conflict
raised in that window surfaces as an error from dequeue instead of an
empty batch.

## Offline converge script

`pgq sql upgrade` has no connection, so it re-states every object behind
guards that make a rerun a no-op: `IF NOT EXISTS` on tables, columns and
indexes, a `DO` block around `CREATE TYPE`, and catalog-guarded `DO`
blocks for the statistics retype, the id widening and the redefined
indexes. Those last are a hand-kept list, `redefined_indexes`: a new
index redefinition must be added there, or the offline script keeps the
old definition while the connected upgrade rebuilds it.

The script is a weaker superset. It never refuses, reports nothing
beyond a SQL comment over retired columns, does not bring `NOT NULL`,
unique constraints or column kinds into line, and widens only tables
with a serial `id`, so the log table's identity `id` is never widened
offline.
It builds its settings from flags, so `PGQUEUER_DURABILITY` and
`PGQUEUER_WIDEN_ID` are ignored.

## Invariants

- A converged database plans zero statements, including after a rerun.
- The planner never emits `DROP TABLE` or `DROP COLUMN`, and touches only
  declared, retired and rebuild names.
- A refusal applies nothing and releases the lock.

## Guards

`test_schema_inspect.py` reads back a fresh install and requires it to
equal the declaration. `test_schema_convergence.py` installs every
release in `test/schema_releases/`, upgrades it, and requires the result
to contain the declaration, plan nothing further, run a job and
aggregate statistics. `test_schema_plan.py` covers planner outcomes and
ordering without a database, `test_schema_index_rebuild.py` the cut-off
rebuilds, `test_schema_upgrade.py` the lock, and
`test_schema_converge_script.py` the offline script. Spelling can change
between PostgreSQL majors, so these run on every supported one.

## Known gaps

Inspection does not read `tgenabled`, trigger timing or `indisvalid`, so
a disabled trigger or an invalid index compares as current. It reads
only the first declared function and trigger. The sequence read is not
bound to the `id` column, so a second, narrow owned sequence makes every
plan re-emit the widening. A rebuild leftover beside a current index is
never dropped, and an absent serial column on an existing table plans
nothing and says nothing.
