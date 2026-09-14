from __future__ import annotations

import pytest

from pgqueuer.adapters.persistence.schema_inspect import inspect
from pgqueuer.db import AsyncpgDriver
from pgqueuer.domain.settings import DBSettings
from pgqueuer.queries import Queries
from test.helpers import collapse, declared_schema, queries_for


async def test_fresh_install_inspects_equal_to_target(apgdriver: AsyncpgDriver) -> None:
    """The model is canonical: what install creates is what target() declares.

    A failure here prints the spelling PostgreSQL reports; paste it into the
    declaration. This is the only thing keeping the model from rotting.
    """
    settings = DBSettings()
    assert collapse(await inspect(apgdriver, settings)) == declared_schema(settings)


@pytest.mark.parametrize("durability", ["durable", "volatile"])
async def test_durability_round_trips(apgdriver: AsyncpgDriver, durability: str) -> None:
    settings = DBSettings(durability=durability)  # type: ignore[arg-type]
    await Queries(apgdriver).uninstall()
    await queries_for(apgdriver, settings).install()
    assert collapse(await inspect(apgdriver, settings)) == declared_schema(settings)


async def test_prefixed_and_schema_scoped_install(apgdriver: AsyncpgDriver) -> None:
    settings = DBSettings(prefix="acme_", db_schema="billing")
    await queries_for(apgdriver, settings).install()
    assert collapse(await inspect(apgdriver, settings)) == declared_schema(settings)


async def test_missing_namespace_is_reported(apgdriver: AsyncpgDriver) -> None:
    settings = DBSettings(db_schema="absent_schema")
    schema = await inspect(apgdriver, settings)
    assert schema.namespace_exists is False
    assert schema.tables == ()
