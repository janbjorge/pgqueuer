# SQL naming model

How dynamically built statements get their object names, and where
values go instead. How a statement is *assembled* is the
[dequeue composition model](dequeue-composition.md). It is a sub-model
of the [system design](README.md).

## Flow

```
┌───────────────┐  prefix, db_schema, per-object overrides,
│  DBSettings   │  from arguments or PGQUEUER_* environment
└───────┬───────┘
        │ validated once
        ▼
┌───────────────┐  bare:      pgqueuer_log
│ Name spellings│  qualified: app.pgqueuer_log
│               │  namespace: 'app' or current_schema()
└───────┬───────┘
        │ interpolated as identifiers
        ▼
┌───────────────┐  values bound as $N, never interpolated
│   Statement   │
└───────┬───────┘
        ▼
┌───────────────┐
│    Driver     │
└───────────────┘
```

Every object name defaults to `prefix` plus a base name (`pgqueuer`,
`pgqueuer_log`, `pgqueuer_statistics`, `pgqueuer_schedules`,
`pgqueuer_status`, `fn_pgqueuer_changed`, `tg_pgqueuer_changed`,
`ch_pgqueuer`) and can be overridden on its own. Index names derive from
their table. `prefix` and `db_schema` must be plain identifiers, and
`db_schema` is lowercased so its identifier and string-literal uses
agree. `qualified` is cached on first use, so a `DBSettings` must not
change after it is used.

## Spellings

One name appears in three spellings, and most bugs in this layer come
from using the wrong one.

The bare name is what the declaration holds and what the catalog is
compared against. DDL uses it where it names something new: `CREATE
INDEX`, `RENAME TO`, a trigger. The qualified name,
`settings.qualify(name)` or `settings.qualified.<name>`, is used for
every reference to an existing table, type, function or index. With
`db_schema` unset it is the bare name. The namespace expression,
`settings.schema_expr`, is `'<db_schema>'` or `current_schema()` and
scopes catalog queries and `DO` blocks.

The status enum is the awkward one, because declared expressions mention
it inside casts. `schema_ddl.spelled` qualifies it when rendering, and
only as a whole column type or after `::`, so a column that shares its
name stays bare. `schema_inspect.unqualify` strips `<db_schema>.` from
what the catalog reports. The connected planner therefore always
compares bare text; the offline index guard is the one place that
compares qualified text.

The NOTIFY channel is database-wide and never qualified. With
`db_schema` unset, statements resolve through the whole `search_path`,
but catalog reads use `current_schema()`, so an object in a later schema
works at runtime and still inspects as absent.

## Identifiers and values

Runtime values (ids, entrypoints, limits, payloads, intervals) are always
bound, through `SqlComposer.bind` or a fixed placeholder list; catalog
lookups bind their name lists too. What is interpolated comes from
settings, from the declaration, or from a closed `Literal` such as the
sort order. That includes object names used as string literals, in
`pg_get_serial_sequence('…')`, `hashtext('…')` and the offline guards,
and everything inside a `DO` block, which cannot take parameters.

## Restated definitions

Some statements must repeat what the declaration owns. Enqueue's `ON
CONFLICT` restates the dedupe index predicate and reads it from
`queue.dedupe_predicate`; the statistics bucket fold reads
`statistics.UNIQUE_COUNT_KEY`. Several places still restate by hand: the
log aggregation and schedules `ON CONFLICT` targets, the startup checks
in `QueueManager.verify_structure` and `SchedulerManager.run`, `pgq
verify`, the slot index name in `Queries.dequeue`, the offline
`redefined_indexes` list, and the `id` and `status` column names in the
widening and retype blocks. Renaming an object in the declaration means
updating each of them.

## Lock keys

The upgrade lock is session-scoped and keyed on `crc32` of the qualified
queue table; log aggregation takes a transaction lock on `hashtext` of
the qualified statistics table. Installations sharing a database contend
only when those names match, which includes same-named installations
separated only by `search_path`.

## Invariants

- No runtime value is interpolated into SQL text.
- The declaration and the catalog meet bare; qualification happens only
  when rendering.

## Guards

`test_schema_support.py` runs install, upgrade, dedupe, widening and the
dashboard queries under `db_schema` and a prefix, and checks the channel
stays unqualified. `test_schema_render.py` and `test_schema_model.py`
check qualification and prefixes on rendered and declared names.
`test_composer.py` and `test_dequeue_shapes.py` cover placeholder
numbering, and `test_cli_sql.py` the offline SQL.

## Known gaps

Per-object overrides are interpolated as given, apart from a dot check
under `db_schema` that skips the channel and trigger. Names render
unquoted, so PostgreSQL lowercases and truncates them at 63 bytes while
catalog lookups bind them as configured; an uppercase or very long
prefix makes inspection miss objects that exist. With `db_schema` unset,
the enum label check scans every schema.
