from __future__ import annotations

import sys
from typing import Any, Callable
from unittest.mock import Mock

import pytest

from pgqueuer.factories import load_factory, run_factory


def test_load_factory_with_module_colon_function(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Test loading a factory using the 'module:function' syntax.
    """
    factory_path: str = "test_module:test_factory"

    # Create a mock factory function
    mock_factory: Mock = Mock(return_value="factory_result")
    # Create a mock module and assign the mock factory to it
    mock_module: Mock = Mock(spec_set=["test_factory"])
    mock_module.test_factory = mock_factory

    # Define a mock import_module function
    def mock_import_module(module_name: str) -> Any:
        if module_name == "test_module":
            return mock_module
        raise ImportError(f"No module named '{module_name}'")

    # Patch 'importlib.import_module' with the mock_import_module
    monkeypatch.setattr("importlib.import_module", mock_import_module)

    # Call the function under test
    factory: Callable[..., Any] = load_factory(factory_path)

    # Assertions
    assert factory is mock_factory, "The loaded factory should match the mock factory."


def test_load_factory_nonexistent_module(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Test loading a factory from a non-existent module.
    Expect an ImportError.
    """
    factory_path: str = "nonexistent_module:test_factory"

    # Define a mock import_module function that raises ImportError
    def mock_import_module(module_name: str) -> Any:
        raise ImportError(f"No module named '{module_name}'")

    # Patch 'importlib.import_module' with the mock_import_module
    monkeypatch.setattr("importlib.import_module", mock_import_module)

    with pytest.raises(ImportError, match="No module named 'nonexistent_module'"):
        load_factory(factory_path)


def test_load_factory_nonexistent_function(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Test loading a non-existent factory function from an existing module.
    Expect an AttributeError.
    """
    factory_path: str = "test_module:nonexistent_factory"

    # Create a mock module with only 'test_factory' attribute
    mock_module: Mock = Mock(spec_set=["test_factory"])
    mock_module.test_factory = Mock(return_value="factory_result")

    # Define a mock import_module function
    def mock_import_module(module_name: str) -> Any:
        if module_name == "test_module":
            return mock_module
        raise ImportError(f"No module named '{module_name}'")

    # Patch 'importlib.import_module' with the mock_import_module
    monkeypatch.setattr("importlib.import_module", mock_import_module)

    with pytest.raises(AttributeError, match="Mock object has no attribute 'nonexistent_factory'"):
        load_factory(factory_path)


def test_load_factory_sys_path_modified(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Ensure that the current working directory is inserted into sys.path.
    """
    factory_path: str = "test_module:test_factory"

    # Create a mock factory function
    mock_factory: Mock = Mock(return_value="factory_result")
    # Create a mock module and assign the mock factory to it
    mock_module: Mock = Mock(spec_set=["test_factory"])
    mock_module.test_factory = mock_factory

    # Define a mock import_module function
    def mock_import_module(module_name: str) -> Any:
        if module_name == "test_module":
            return mock_module
        raise ImportError(f"No module named '{module_name}'")

    # Patch 'importlib.import_module' and 'os.getcwd'
    monkeypatch.setattr("importlib.import_module", mock_import_module)
    monkeypatch.setattr("os.getcwd", lambda: "/fake/dir")

    # Capture the original sys.path
    original_sys_path: list[str] = list(sys.path)

    try:
        # Call the function under test
        load_factory(factory_path)

        # Assertions
        assert sys.path[0] == "/fake/dir", (
            "The current working directory should be inserted at sys.path[0]."
        )
    finally:
        # Restore sys.path to its original state
        sys.path = original_sys_path


async def test_run_factory_with_async_context_manager() -> None:
    """
    Test run_factory with an asynchronous context manager.
    """
    from contextlib import AbstractAsyncContextManager

    class AsyncCM(Mock, AbstractAsyncContextManager[Any]):
        async def __aenter__(self) -> str:
            return "async_cm_value"

        async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
            pass

    async_cm: AsyncCM = AsyncCM()

    async with run_factory(async_cm) as value:
        assert value == "async_cm_value", (
            "The async context manager should yield the correct value."
        )


async def test_run_factory_with_sync_context_manager(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Test run_factory with a synchronous context manager.
    """
    from contextlib import AbstractContextManager

    class SyncCM(Mock, AbstractContextManager[Any]):
        def __enter__(self) -> str:
            return "sync_cm_value"

        def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
            pass

    sync_cm: SyncCM = SyncCM()

    async with run_factory(sync_cm) as value:
        assert value == "sync_cm_value", "The sync context manager should yield the correct value."


async def test_run_factory_with_awaitable() -> None:
    """
    Test run_factory with an awaitable.
    """

    async def sample_awaitable() -> str:
        return "awaitable_value"

    awaitable: Any = sample_awaitable()

    async with run_factory(awaitable) as value:
        assert value == "awaitable_value", "The awaitable should yield the awaited value."


async def test_run_factory_with_invalid_input() -> None:
    """
    Test run_factory with an invalid input that is neither an awaitable nor a context manager.
    Expect a TypeError.
    """
    invalid_input: str = "not a valid input"

    with pytest.raises(TypeError, match="object str can't be used in 'await' expression"):
        async with run_factory(invalid_input):  # type: ignore[arg-type]
            pass  # This line should not be reached
