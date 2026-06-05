from __future__ import annotations

import anyio
import pytest

from pgqueuer.models import Context, ResourceKey, ScheduleContext


class Pool:
    pass


DB = ResourceKey("db", Pool)


async def test_resource_returns_typed_value() -> None:
    pool = Pool()
    ctx = Context(cancellation=anyio.CancelScope(), resources={DB.name: pool})
    assert ctx.resource(DB) is pool


async def test_resource_missing_key_raises_keyerror() -> None:
    ctx = Context(cancellation=anyio.CancelScope())
    with pytest.raises(KeyError, match="not registered"):
        ctx.resource(DB)


async def test_resource_wrong_type_raises_typeerror() -> None:
    ctx = Context(cancellation=anyio.CancelScope(), resources={DB.name: "not a pool"})
    with pytest.raises(TypeError, match="expected Pool"):
        ctx.resource(DB)


async def test_resources_dict_still_accessible() -> None:
    """The token is additive: bare-string access keeps working."""
    pool = Pool()
    ctx = Context(cancellation=anyio.CancelScope(), resources={"db": pool})
    assert ctx.resources["db"] is pool


def test_schedule_context_resource() -> None:
    pool = Pool()
    ctx = ScheduleContext(resources={DB.name: pool})
    assert ctx.resource(DB) is pool


def test_resource_key_resolve_directly() -> None:
    pool = Pool()
    assert DB.resolve({DB.name: pool}) is pool
