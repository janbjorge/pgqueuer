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
   - Sync drivers could be used as well but support a smaller set of operations.

5. **Default Isolation Level**:
   - Connections should maintain the default PostgreSQL isolation level unless explicitly modified.

## Implementation Notes
- PGQueuer includes runtime checks to verify critical connection properties, such as autocommit mode, and provides descriptive error messages when requirements are not met.
- The `drivers.py` file defines the abstractions and utility functions necessary for seamless database integration.

## Future Enhancements
Planned improvements include:

- Adding support for more database drivers and connection methods.
- Enhancing error diagnostics to simplify troubleshooting.
- Introducing more robust abstractions to make driver integration easier.

