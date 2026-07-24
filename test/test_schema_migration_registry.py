"""Guards for the append-only schema migration registry (qb.Migration).

Two invariants keep ``pgq upgrade`` honest:

1. Shipped migrations are frozen. Editing an already-released migration in
   place silently no-ops on deployments that ran the old statement, so a
   content-hash pin forces any schema change to append a *new* migration.
2. Fresh install and the migration sequence stay in sync: running ``upgrade``
   on a current install must be a structural no-op, or install-DDL and the
   migration list have drifted.
"""

from __future__ import annotations

import hashlib

from pgqueuer import db, queries
from pgqueuer.adapters.persistence.qb import Migration, QueryBuilderEnvironment
from pgqueuer.domain.settings import DBSettings

# Content hash of every shipped migration, rendered under canonical defaults.
# APPEND ONLY. A failing assertion here means a released migration was edited
# or reordered -- append a new migration instead of changing an old one, then
# add its id/hash below.
FROZEN_MIGRATION_HASHES: dict[int, str] = {
    1: "f2ac1f739e1e7eb8a329efcdaecad80caf7224e5be3b59b8be124fac3562ccb7",
    2: "ac2540ca5d841b0115d6171e59491efdcce127d4edf24a70490ca1f9e01b0d4a",
    3: "a2ece84c5d7a2e3190795ca1d42d37e890b948147d03d1c8a66f734223b612cc",
    4: "b59f85c36bc57bd5a756cc2801b28a9506b665798c2cb81ba11079466773e60a",
    5: "4aba1c90a6dac256b32c6b2e4354995d698a3b80c34b9e7418b7a2e439ea7a4b",
    6: "bfb1b21278327b6f0eab400ad9bd0272fa80a2d73b6c847697881398f79eeb63",
    7: "3a26e274625295437d9e40011499bf6634f90a8c01663bee5a693b72a10426ed",
    8: "67c37ac059a2bb7392855721c01c9533e24606806f4d53fcad616c0dd539c044",
    9: "9617bad69872dd86ee6c366841c989426d0fe881e0bb88ca9865e3515c1bfdf4",
    10: "39ac93f3d991428d4d019c895196870de6f5d1401a850f1a78cba8055ec73090",
    11: "212e5253636c3a7db183f3abbca3d8414d8251bd00c532171fa00b32765de970",
    12: "74388340913081fe9059ead4d113f2dcb2e83bd4b49c1003cea3a4e16c6b5b6b",
    13: "f71711252cd93efb310f849245b569bd254b9e9b9893475bbb10952d7db67c0a",
    14: "69178fc669dbd7f62f4915a9f90f2960e40dee0103bdbb2ab8c13a56e7881aa5",
    15: "d57fb0e380316d70265ca2d6ffc32cedeca979a54789a6732b74b32592dbd6c8",
    16: "5d31fd9c250851c78b245e29776c64a0c70c20aef22320fd95f6ee2ec845e3fd",
    17: "aec7e02d4297d649be1ae72f3839468d081e5e07d38d3b6e51fad21dd45d1494",
    18: "414c4d61806db1ad54679f24bb24cf00b53a584b3ba3f9d638192a916429f007",
}


def migration_hash(migration: Migration, env: QueryBuilderEnvironment) -> str:
    """Stable content hash over id, description, and rendered SQL."""
    body = "\x00".join([str(migration.id), migration.description, *migration.render(env)])
    return hashlib.sha256(body.encode()).hexdigest()


def test_shipped_migrations_are_frozen() -> None:
    """Every released migration renders to its pinned hash (append-only enforcement)."""
    env = QueryBuilderEnvironment(DBSettings())
    rendered = {m.id: migration_hash(m, env) for m in env.migrations()}
    assert rendered == FROZEN_MIGRATION_HASHES


async def schema_fingerprint(driver: db.Driver) -> dict[str, list[tuple[str, ...]]]:
    """Structural snapshot of PgQueuer objects: columns, indexes, and enum labels."""
    s = DBSettings()
    tables = (s.queue_table, s.queue_table_log, s.statistics_table, s.schedules_table)

    columns = await driver.fetch(
        """SELECT table_name, column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema = current_schema() AND table_name = ANY($1)
        ORDER BY table_name, column_name;""",
        list(tables),
    )
    indexes = await driver.fetch(
        """SELECT indexname, indexdef FROM pg_indexes
        WHERE schemaname = current_schema() AND tablename = ANY($1)
        ORDER BY indexname;""",
        list(tables),
    )
    enums = await driver.fetch(
        """SELECT t.typname, e.enumlabel
        FROM pg_enum e
        JOIN pg_type t ON e.enumtypid = t.oid
        JOIN pg_namespace n ON t.typnamespace = n.oid
        WHERE n.nspname = current_schema() AND t.typname = $1
        ORDER BY e.enumsortorder;""",
        s.queue_status_type,
    )
    return {
        "columns": [tuple(str(v) for v in r.values()) for r in columns],
        "indexes": [tuple(str(v) for v in r.values()) for r in indexes],
        "enums": [tuple(str(v) for v in r.values()) for r in enums],
    }


async def test_upgrade_is_a_structural_noop_on_current_install(apgdriver: db.Driver) -> None:
    """Fresh install and the migration sequence agree: upgrade changes nothing."""
    q = queries.Queries(apgdriver)
    before = await schema_fingerprint(apgdriver)

    await q.upgrade()

    after = await schema_fingerprint(apgdriver)
    assert before == after
