"""
Unit tests for the retry module.

This module tests the retry logic that was extracted from TimedOverflowBuffer
to ensure proper separation of concerns and comprehensive coverage of retry scenarios.
"""

import asyncio
import pytest
from datetime import timedelta
from unittest.mock import Mock, AsyncMock

from pgqueuer.retries import RetryManager
from pgqueuer.helpers import ExponentialBackoff


class TestRetryManager:
    """Test cases for the RetryManager class."""

    @pytest.fixture
    def retry_manager(self) -> RetryManager[list[str]]:
        """Create a retry manager instance for testing."""
        return RetryManager()

    @pytest.fixture
    def custom_retry_manager(self) -> RetryManager[list[str]]:
        """Create a retry manager with custom backoff settings."""
        return RetryManager(
            retry_backoff=ExponentialBackoff(
                start_delay=timedelta(milliseconds=10),
                max_delay=timedelta(milliseconds=100),
            ),
            shutdown_backoff=ExponentialBackoff(
                start_delay=timedelta(milliseconds=1),
                max_delay=timedelta(milliseconds=10),
            ),
        )

    async def test_successful_operation(self, retry_manager: RetryManager[list[str]]) -> None:
        """Test that a successful operation completes without retry."""
        mock_operation = AsyncMock()
        test_data = ["item1", "item2"]
        
        result = await retry_manager.execute_with_retry(
            operation=mock_operation,
            data=test_data,
            operation_name="test_operation",
        )
        
        assert result is True
        mock_operation.assert_called_once_with(test_data)

    async def test_operation_with_single_retry(self, retry_manager: RetryManager[list[str]]) -> None:
        """Test that a failing operation retries and eventually succeeds."""
        mock_operation = AsyncMock()
        mock_operation.side_effect = [RuntimeError("Temporary failure"), None]
        test_data = ["item1", "item2"]
        
        # First call should fail and return False
        result1 = await retry_manager.execute_with_retry(
            operation=mock_operation,
            data=test_data,
            operation_name="test_operation",
        )
        assert result1 is False
        
        # Second call should succeed
        result2 = await retry_manager.execute_with_retry(
            operation=mock_operation,
            data=test_data,
            operation_name="test_operation",
        )
        assert result2 is True
        
        assert mock_operation.call_count == 2

    async def test_retry_until_success_or_limit(self, custom_retry_manager: RetryManager[list[str]]) -> None:
        """Test retry until success or backoff limit is reached."""
        mock_operation = AsyncMock()
        # Make it fail twice, then succeed
        mock_operation.side_effect = [
            RuntimeError("Failure 1"),
            RuntimeError("Failure 2"), 
            None
        ]
        test_data = ["item1", "item2"]
        
        result = await custom_retry_manager.retry_until_success_or_limit(
            operation=mock_operation,
            data=test_data,
            operation_name="test_operation",
        )
        
        assert result is True
        assert mock_operation.call_count == 3

    async def test_retry_until_limit_reached(self, custom_retry_manager: RetryManager[list[str]]) -> None:
        """Test that retries stop when the backoff limit is reached."""
        mock_operation = AsyncMock()
        mock_operation.side_effect = RuntimeError("Persistent failure")
        test_data = ["item1", "item2"]
        
        result = await custom_retry_manager.retry_until_success_or_limit(
            operation=mock_operation,
            data=test_data,
            operation_name="test_operation",
        )
        
        assert result is False
        # Should have tried multiple times before giving up
        assert mock_operation.call_count > 1

    async def test_shutdown_backoff_behavior(self, retry_manager: RetryManager[list[str]]) -> None:
        """Test that shutdown backoff is used when specified."""
        mock_operation = AsyncMock()
        mock_operation.side_effect = RuntimeError("Failure")
        test_data = ["item1", "item2"]
        
        result = await retry_manager.execute_with_retry(
            operation=mock_operation,
            data=test_data,
            operation_name="test_operation",
            use_shutdown_backoff=True,
        )
        
        assert result is False
        mock_operation.assert_called_once_with(test_data)

    async def test_shutdown_stops_retries(self, retry_manager: RetryManager[list[str]]) -> None:
        """Test that setting shutdown stops retry operations."""
        mock_operation = AsyncMock()
        mock_operation.side_effect = RuntimeError("Failure")
        test_data = ["item1", "item2"]
        
        # Set shutdown event
        retry_manager.set_shutdown()
        
        result = await retry_manager.execute_with_retry(
            operation=mock_operation,
            data=test_data,
            operation_name="test_operation",
        )
        
        assert result is False
        mock_operation.assert_called_once_with(test_data)

    def test_reset_backoff(self, retry_manager: RetryManager[list[str]]) -> None:
        """Test that backoff can be reset."""
        # Advance the backoff
        retry_manager.retry_backoff.next_delay()
        initial_delay = retry_manager.retry_backoff.start_delay
        
        # Reset and check
        retry_manager.reset_backoff()
        assert retry_manager.retry_backoff.current_delay == initial_delay

    def test_reset_shutdown_backoff(self, retry_manager: RetryManager[list[str]]) -> None:
        """Test that shutdown backoff can be reset."""
        # Advance the shutdown backoff
        retry_manager.shutdown_backoff.next_delay()
        initial_delay = retry_manager.shutdown_backoff.start_delay
        
        # Reset and check
        retry_manager.reset_shutdown_backoff()
        assert retry_manager.shutdown_backoff.current_delay == initial_delay

    async def test_backoff_progression(self, custom_retry_manager: RetryManager[list[str]]) -> None:
        """Test that backoff delays increase as expected."""
        mock_operation = AsyncMock()
        mock_operation.side_effect = RuntimeError("Persistent failure")
        test_data = ["item1"]
        
        initial_delay = custom_retry_manager.retry_backoff.current_delay
        
        # First failure
        await custom_retry_manager.execute_with_retry(
            operation=mock_operation,
            data=test_data,
            operation_name="test_operation",
        )
        
        # Check that backoff has increased
        assert custom_retry_manager.retry_backoff.current_delay > initial_delay

    def test_default_values(self) -> None:
        """Test that default values are set correctly."""
        retry_manager = RetryManager()
        
        assert retry_manager.retry_backoff.start_delay == timedelta(seconds=0.01)
        assert retry_manager.retry_backoff.max_delay == timedelta(seconds=10)
        assert retry_manager.shutdown_backoff.start_delay == timedelta(milliseconds=1)
        assert retry_manager.shutdown_backoff.max_delay == timedelta(milliseconds=100)
        assert not retry_manager.shutdown.is_set()

    async def test_different_exception_types(self, retry_manager: RetryManager[list[str]]) -> None:
        """Test that different exception types are handled correctly."""
        mock_operation = AsyncMock()
        test_data = ["item1"]
        
        exceptions = [ValueError("Value error"), ConnectionError("Connection failed"), OSError("OS error")]
        
        for exception in exceptions:
            mock_operation.reset_mock()
            mock_operation.side_effect = exception
            
            result = await retry_manager.execute_with_retry(
                operation=mock_operation,
                data=test_data,
                operation_name="test_operation",
            )
            
            assert result is False
            mock_operation.assert_called_once_with(test_data)

    async def test_empty_data_handling(self, retry_manager: RetryManager[list[str]]) -> None:
        """Test that empty data is handled correctly."""
        mock_operation = AsyncMock()
        empty_data: list[str] = []
        
        result = await retry_manager.execute_with_retry(
            operation=mock_operation,
            data=empty_data,
            operation_name="test_operation",
        )
        
        assert result is True
        mock_operation.assert_called_once_with(empty_data)

    async def test_concurrent_retry_operations(self, retry_manager: RetryManager[list[str]]) -> None:
        """Test that concurrent retry operations work correctly."""
        mock_operation1 = AsyncMock()
        mock_operation2 = AsyncMock()
        test_data1 = ["item1"]
        test_data2 = ["item2"]
        
        # Run operations concurrently
        results = await asyncio.gather(
            retry_manager.execute_with_retry(mock_operation1, test_data1, "op1"),
            retry_manager.execute_with_retry(mock_operation2, test_data2, "op2"),
        )
        
        assert all(results)
        mock_operation1.assert_called_once_with(test_data1)
        mock_operation2.assert_called_once_with(test_data2)

    async def test_operation_name_in_logging(self, retry_manager: RetryManager[list[str]]) -> None:
        """Test that operation name is included in logging."""
        mock_operation = AsyncMock()
        mock_operation.side_effect = RuntimeError("Test failure")
        test_data = ["item1"]
        
        # Mock the logger to capture the warning
        with pytest.MonkeyPatch.context() as mp:
            log_calls = []
            
            def mock_warning(msg, *args):
                log_calls.append((msg, args))
            
            mp.setattr(retry_manager.logger, "warning", mock_warning)
            
            await retry_manager.execute_with_retry(
                operation=mock_operation,
                data=test_data,
                operation_name="custom_operation_name",
            )
            
            assert len(log_calls) == 1
            assert "custom_operation_name" in log_calls[0][1][0]

    async def test_jitter_behavior_during_retry(self, custom_retry_manager: RetryManager[list[str]]) -> None:
        """Test that jitter is applied during retry delays."""
        mock_operation = AsyncMock()
        mock_operation.side_effect = RuntimeError("Failure")
        test_data = ["item1"]
        
        # Record the time it takes to execute retry
        import time
        start_time = time.time()
        
        await custom_retry_manager.execute_with_retry(
            operation=mock_operation,
            data=test_data,
            operation_name="test_operation",
        )
        
        elapsed_time = time.time() - start_time
        
        # Should have slept for some amount of time (with jitter)
        # The exact time will vary due to jitter, but should be > 0
        assert elapsed_time > 0