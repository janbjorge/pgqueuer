from __future__ import annotations

import importlib
import os
import sys
from contextlib import AbstractAsyncContextManager, AbstractContextManager, asynccontextmanager
from typing import Any, AsyncGenerator, Awaitable, Callable, TypeVar

T = TypeVar("T")


def load_factory(factory_path: str) -> Callable[..., Any]:
    """
    Load factory function from a given module path or factory-style path.

    Args:
        factory_path (str): Module path to the factory function or factory-style path.

    Returns: A callable
    """
    sys.path.insert(0, os.getcwd())

    module_name, factory_name = factory_path.split(":", 1)
    module = importlib.import_module(module_name)
    return getattr(module, factory_name)


@asynccontextmanager
async def run_factory(
    factory_result: Awaitable[T] | AbstractContextManager[T] | AbstractAsyncContextManager[T],
) -> AsyncGenerator[T, None]:
    """
    Converts the result of a factory function in a async context manager
    """

    # Check if it's an async context manager
    if isinstance(factory_result, AbstractAsyncContextManager):
        async with factory_result as value:
            yield value
    # Check if it's a synchronous context manager
    elif isinstance(factory_result, AbstractContextManager):
        with factory_result as value:
            yield value
    # Otherwise, assume it's an awaitable and return the result
    else:
        yield await factory_result
