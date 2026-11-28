-- statement 1
SELECT name, tbl.relpersistence = 'u' AS unlogged
FROM unnest($1::text[]) AS name
JOIN pg_class tbl ON tbl.oid = to_regclass(name)

-- statement 2
ALTER TABLE pgqueuer SET LOGGED;

-- statement 3
ALTER TABLE pgqueuer_log SET UNLOGGED;

-- statement 4
ALTER TABLE pgqueuer_statistics SET UNLOGGED;

-- statement 5
ALTER TABLE pgqueuer_schedules SET LOGGED;
