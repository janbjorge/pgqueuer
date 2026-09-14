from __future__ import annotations

import dataclasses

import pytest

from pgqueuer.adapters.persistence.schema_inspect import fold_timezone_calls, inspect
from pgqueuer.db import AsyncpgDriver
from pgqueuer.domain.schema.declaration import target
from pgqueuer.domain.schema.model import Schema, resolve
from pgqueuer.domain.settings import DBSettings
from pgqueuer.domain.types import TypeName
from pgqueuer.queries import Queries
from test.helpers import queries_for


def collapse(schema: Schema) -> Schema:
    """Whitespace-normalise function bodies; everything else compares exactly.

    A plpgsql body cannot be normalised in the model itself: it carries ``--``
    comments, so folding newlines would swallow the rest of each line.
    """
    return dataclasses.replace(
        schema,
        functions=tuple(
            dataclasses.replace(function, body=" ".join(function.body.split()))
            for function in schema.functions
        ),
    )


def declared(settings: DBSettings) -> Schema:
    return collapse(resolve(target(settings), TypeName(settings.queue_status_type)))


async def test_fresh_install_inspects_equal_to_target(apgdriver: AsyncpgDriver) -> None:
    """The model is canonical: what install creates is what target() declares.

    A failure here prints the spelling PostgreSQL reports; paste it into the
    declaration. This is the only thing keeping the model from rotting.
    """
    settings = DBSettings()
    assert collapse(await inspect(apgdriver, settings)) == declared(settings)


@pytest.mark.parametrize("durability", ["durable", "volatile"])
async def test_durability_round_trips(apgdriver: AsyncpgDriver, durability: str) -> None:
    settings = DBSettings(durability=durability)  # type: ignore[arg-type]
    await Queries(apgdriver).uninstall()
    await queries_for(apgdriver, settings).install()
    assert collapse(await inspect(apgdriver, settings)) == declared(settings)


async def test_prefixed_and_schema_scoped_install(apgdriver: AsyncpgDriver) -> None:
    settings = DBSettings(prefix="acme_", db_schema="billing")
    await queries_for(apgdriver, settings).install()
    assert collapse(await inspect(apgdriver, settings)) == declared(settings)


@pytest.mark.parametrize(
    "thirteen, fourteen_plus",
    [
        (
            "date_trunc('sec'::text, timezone('UTC'::text, now()))",
            "date_trunc('sec'::text, (now() AT TIME ZONE 'UTC'::text))",
        ),
        (
            "USING btree (priority, date_trunc('sec'::text, timezone('UTC'::text, created)), status, entrypoint)",  # noqa: E501
            "USING btree (priority, date_trunc('sec'::text, (created AT TIME ZONE 'UTC'::text)), status, entrypoint)",  # noqa: E501
        ),
        ("USING btree (created)", "USING btree (created)"),
    ],
)
def test_postgres_13_timezone_spelling_folds(thirteen: str, fourteen_plus: str) -> None:
    """PostgreSQL 14 changed how AT TIME ZONE is deparsed; 13 folds onto 14+.

    Verbatim catalog output, so the fold is covered on every major rather than
    only when the PG 13 job runs.
    """
    assert fold_timezone_calls(thirteen) == fourteen_plus
    assert fold_timezone_calls(fourteen_plus) == fourteen_plus


async def test_missing_namespace_is_reported(apgdriver: AsyncpgDriver) -> None:
    settings = DBSettings(db_schema="absent_schema")
    schema = await inspect(apgdriver, settings)
    assert schema.namespace_exists is False
    assert schema.tables == ()
