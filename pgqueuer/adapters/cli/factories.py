from __future__ import annotations

import importlib
import inspect
import os
import sys
from contextlib import AbstractAsyncContextManager
from typing import Protocol


class Factory(Protocol):
    """User factory callable; CLI extra arguments are forwarded positionally."""

    def __call__(self, *args: object) -> object: ...


def load_factory(factory: str | Factory) -> Factory:
    """Resolve a factory: a ``module:attr`` import path, or pass-through if already callable."""
    sys.path.insert(0, os.getcwd())

    if callable(factory):
        return factory

    module_name, factory_name = factory.split(":", 1)
    module = importlib.import_module(module_name)
    loaded: object = getattr(module, factory_name)
    if not callable(loaded):
        raise TypeError(f"{factory!r} does not resolve to a callable, got {type(loaded).__name__}")
    return loaded


def validate_factory_result(result: object) -> AbstractAsyncContextManager[object]:
    """Validate that a factory produced an async context manager.

    Raises TypeError with actionable migration instructions when the result
    is not an async context manager.
    """
    if isinstance(result, AbstractAsyncContextManager):
        return result

    if inspect.iscoroutine(result):
        result.close()

    raise TypeError(
        f"Factory must return an async context manager (AsyncContextManager),\n"
        f"but returned {type(result).__name__!r}.\n"
        "\n"
        "Example:\n"
        "\n"
        "    from contextlib import asynccontextmanager\n"
        "\n"
        "    @asynccontextmanager\n"
        "    async def my_factory():\n"
        "        manager = ...  # your setup code\n"
        "        yield manager\n"
        "        # optional cleanup\n"
    )
