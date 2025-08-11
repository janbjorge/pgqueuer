# Drivers Documentation

## Overview
Drivers in PGQueuer act as the bridge between the application logic and the PostgreSQL database, handling the communication and ensuring that operations adhere to PGQueuer's job queueing requirements.

## Purpose of Drivers
Drivers simplify and standardize database interactions by:

- Managing database connections and ensuring correct configurations (e.g., autocommit).
- Abstracting PostgreSQL-specific features to streamline queue operations.
- Providing a consistent interface for executing queries, reducing complexity for users.

## Assumptions for Using a Driver
To ensure smooth integration with PGQueuer, the following requirements must be met:

### Checklist of Requirements for Database Connections

1. **Autocommit Mode**:
   - The database connection must operate in autocommit mode. For psycopg, explicitly enable it using `connection.autocommit = True`.
   - For asyncpg, autocommit is the default behavior unless a transaction is explicitly started, so no additional configuration is needed.

2. **PostgreSQL Compatibility**:
   - The driver must support PostgreSQL-specific features and extensions used by PGQueuer.

3. **Asynchronous Operations** (if applicable):
   - Async drivers should support asyncio-compatible operations if required for the application setup.
  
4. **Synchronous Operations** (if applicable):
   - Sync drivers are intended only for enqueue operations and support a smaller
     set of features.

5. **Default Isolation Level**:
   - Connections should maintain the default PostgreSQL isolation level unless explicitly modified.

## Choosing the Right Driver

PGQueuer bundles several drivers to accommodate different application styles.

### Asynchronous drivers

- **AsyncpgDriver** – a thin wrapper around a single `asyncpg` connection.
- **AsyncpgPoolDriver** – uses an `asyncpg` connection pool for improved
  throughput.
- **PsycopgDriver** – built on psycopg's async connection API. A minimal
  setup looks like:

  ```python
  import psycopg
  from pgqueuer.db import PsycopgDriver

  conn = await psycopg.AsyncConnection.connect(dsn)
  conn.autocommit = True
  driver = PsycopgDriver(conn)
  ```

### Synchronous driver (enqueue only)

- **SyncPsycopgDriver** – designed for blocking code or frameworks such as
  Flask. It can only enqueue jobs; PGQueuer's consumers and other internals
  require an async driver.

  ```python
  import psycopg
  from pgqueuer.db import SyncPsycopgDriver
  from pgqueuer.queries import Queries

  conn = psycopg.connect(dsn, autocommit=True)
  driver = SyncPsycopgDriver(conn)
  queries = Queries(driver)
  queries.enqueue("fetch", b"payload")
  ```

### Best practices

- Prefer an async driver when your project already runs on asyncio.
- Use the sync driver only to enqueue jobs from short-lived scripts or
  WSGI-style applications.
- Reuse connections or pools and keep autocommit enabled.

## Implementation Notes
- PGQueuer includes runtime checks to verify critical connection properties, such as autocommit mode, and provides descriptive error messages when requirements are not met.
- The `drivers.py` file defines the abstractions and utility functions necessary for seamless database integration.

## Future Enhancements
Planned improvements include:

- Adding support for more database drivers and connection methods.
- Enhancing error diagnostics to simplify troubleshooting.
- Introducing more robust abstractions to make driver integration easier.

