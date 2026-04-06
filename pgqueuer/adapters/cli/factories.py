from __future__ import annotations

import importlib
import inspect
import os
import sys
from contextlib import AbstractAsyncContextManager
from typing import Any, Callable


def load_factory(factory: str | Callable[..., Any]) -> Callable[..., Any]:
    """
    Load factory function.

    Args:
        factory (str | Callable): Factory function specification. It may be one of the
        following:
          - Module path to the factory function
          - Factory-style path
          - Callable factory function


    Returns: A callable
    """
    sys.path.insert(0, os.getcwd())

    if callable(factory):
        return factory

    module_name, factory_name = factory.split(":", 1)
    module = importlib.import_module(module_name)
    return getattr(module, factory_name)


def validate_factory_result(result: object) -> AbstractAsyncContextManager[Any]:
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
