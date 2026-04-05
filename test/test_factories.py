from __future__ import annotations

import sys
from contextlib import (
    AbstractAsyncContextManager,
    asynccontextmanager,
    contextmanager,
)
from functools import partial
from typing import Any, AsyncGenerator, Callable, Generator
from unittest.mock import Mock

import pytest

from pgqueuer.factories import load_factory, validate_factory_result


def test_load_factory_with_module_colon_function(monkeypatch: pytest.MonkeyPatch) -> None:
    factory_path: str = "test_module:test_factory"

    mock_factory: Mock = Mock(return_value="factory_result")
    mock_module: Mock = Mock(spec_set=["test_factory"])
    mock_module.test_factory = mock_factory

    def mock_import_module(module_name: str) -> Any:
        if module_name == "test_module":
            return mock_module
        raise ImportError(f"No module named '{module_name}'")

    monkeypatch.setattr("importlib.import_module", mock_import_module)

    factory: Callable[..., Any] = load_factory(factory_path)
    assert factory is mock_factory


def test_load_factory_nonexistent_module(monkeypatch: pytest.MonkeyPatch) -> None:
    def mock_import_module(module_name: str) -> Any:
        raise ImportError(f"No module named '{module_name}'")

    monkeypatch.setattr("importlib.import_module", mock_import_module)

    with pytest.raises(ImportError, match="No module named 'nonexistent_module'"):
        load_factory("nonexistent_module:test_factory")


def test_load_factory_nonexistent_function(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_module: Mock = Mock(spec_set=["test_factory"])
    mock_module.test_factory = Mock(return_value="factory_result")

    def mock_import_module(module_name: str) -> Any:
        if module_name == "test_module":
            return mock_module
        raise ImportError(f"No module named '{module_name}'")

    monkeypatch.setattr("importlib.import_module", mock_import_module)

    with pytest.raises(AttributeError, match="Mock object has no attribute 'nonexistent_factory'"):
        load_factory("test_module:nonexistent_factory")


def test_load_factory_sys_path_modified(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_factory: Mock = Mock(return_value="factory_result")
    mock_module: Mock = Mock(spec_set=["test_factory"])
    mock_module.test_factory = mock_factory

    def mock_import_module(module_name: str) -> Any:
        if module_name == "test_module":
            return mock_module
        raise ImportError(f"No module named '{module_name}'")

    monkeypatch.setattr("importlib.import_module", mock_import_module)
    monkeypatch.setattr("os.getcwd", lambda: "/fake/dir")

    original_sys_path: list[str] = list(sys.path)
    try:
        load_factory("test_module:test_factory")
        assert sys.path[0] == "/fake/dir"
    finally:
        sys.path = original_sys_path


def test_load_factory_with_callable() -> None:
    def factory_function(arg: Any) -> Any:
        return arg

    factory_callable = partial(factory_function, arg=42)
    factory_function_ = load_factory(factory_callable)
    assert factory_function_() == 42


# ---------------------------------------------------------------------------
# validate_factory_result — accepts async CM, rejects everything else
# ---------------------------------------------------------------------------


def test_validate_accepts_async_context_manager() -> None:
    @asynccontextmanager
    async def factory() -> AsyncGenerator[str, None]:
        yield "value"

    result = validate_factory_result(factory())
    assert isinstance(result, AbstractAsyncContextManager)


async def test_validate_rejects_coroutine_with_migration_message() -> None:
    async def factory() -> str:
        return "value"

    with pytest.raises(TypeError, match="returned a coroutine") as exc_info:
        validate_factory_result(factory())

    msg = str(exc_info.value)
    assert "@asynccontextmanager" in msg
    assert "yield" in msg


def test_validate_rejects_sync_cm_with_migration_message() -> None:
    @contextmanager
    def factory() -> Generator[str, None, None]:
        yield "value"

    with pytest.raises(TypeError, match="synchronous") as exc_info:
        validate_factory_result(factory())

    msg = str(exc_info.value)
    assert "@asynccontextmanager" in msg


def test_validate_rejects_arbitrary_type_with_migration_message() -> None:
    with pytest.raises(TypeError, match="AsyncContextManager") as exc_info:
        validate_factory_result("not a manager")

    msg = str(exc_info.value)
    assert "@asynccontextmanager" in msg
