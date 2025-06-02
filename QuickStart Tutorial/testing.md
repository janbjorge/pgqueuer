# PgQueuer Testing Documentation

## Overview

This document provides guidelines for testing PgQueuer jobs, ensuring that your job handlers function correctly and efficiently. It covers various testing strategies, including unit tests, integration tests, and performance tests.

## Testing Strategies

### Unit Testing

Unit tests are essential for verifying the functionality of individual job handlers. Use a testing framework like `pytest` to create tests for each job handler.

#### Example Unit Test

```python
import pytest
from pgqueuer import PgQueuer
from pgqueuer.models import Job

@pytest.mark.asyncio
async def test_example_job_handler():
    job = Job(id=1, entrypoint="example_handler", payload=b'{"key": "value"}', priority=0)
    result = await example_handler(job)
    assert result == expected_result
```

### Integration Testing

Integration tests ensure that your job handlers work correctly within the context of the entire PgQueuer system. This includes testing the interaction between job handlers and the database.

#### Example Integration Test

```python
@pytest.mark.asyncio
async def test_integration_with_database():
    # Setup database connection and PgQueuer instance
    pgq = PgQueuer(driver)
    
    # Enqueue a job and process it
    await pgq.enqueue("example_handler", payload)
    
    # Verify job processing
    result = await check_job_status(job_id)
    assert result == "completed"
```

### Performance Testing

Performance tests help you understand how your job handlers perform under load. Use tools like `pytest-benchmark` to measure execution time and throughput.

#### Example Performance Test

```python
@pytest.mark.benchmark
def test_performance_of_job_handler(benchmark):
    job = Job(id=1, entrypoint="performance_handler", payload=b'{"key": "value"}', priority=0)
    benchmark(example_handler, job)
```

## Mocking External Dependencies

When testing job handlers that interact with external services, use mocking to simulate those interactions. This allows you to isolate the job handler's logic.

#### Example Mocking

```python
from unittest.mock import patch

@patch('external_service.call')
def test_job_with_mocked_service(mock_service):
    mock_service.return_value = "mocked response"
    result = await job_handler(job)
    assert result == expected_result
```

## Running Tests

To run your tests, use the following command:

```bash
pytest -v
```

This will execute all tests in your project and provide detailed output.

## Conclusion

Testing is a crucial part of developing reliable job handlers in PgQueuer. By following these guidelines and implementing thorough tests, you can ensure that your jobs function as expected and handle errors gracefully.