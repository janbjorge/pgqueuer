CLI Module for PGQueuer
========================

The **pgq CLI** provides a command-line interface for managing various aspects of the PGQueuer system. The CLI can be invoked via the alias ``pgq`` or the traditional method ``python3 -m pgqueuer``. Both methods are fully supported and provide identical functionality.

Functionality
-------------

The CLI offers commands to install, uninstall, upgrade, and manage the job queue system. Additionally, it provides tools for real-time monitoring through a dashboard and PostgreSQL NOTIFY channels.

Key Commands
------------

``install``
~~~~~~~~~~~
Set up the necessary database schema for PGQueuer.

- **Options**:
  - ``--durability``: Define the durability level for tables. Possible values are:
    - ``volatile``: All tables are unlogged, prioritizing performance over durability.
    - ``balanced``: Critical tables (e.g., ``pgqueuer`` and ``pgqueuer_schedules``) are logged, while auxiliary tables are unlogged.
    - ``durable``: All tables are logged for maximum durability.
    - **Default**: ``durable``.

**Example**::

    pgq install --durability balanced

``uninstall``
~~~~~~~~~~~~~
Remove the PGQueuer schema from the database.

``upgrade``
~~~~~~~~~~~
Apply database schema upgrades to PGQueuer.

- **Options**:
  - ``--durability``: Adjust the durability level during the upgrade process, with the same options as ``install``.

**Example**::

    pgq upgrade --durability durable

``verify``
~~~~~~~~~~
Ensure PGQueuer tables, triggers, and functions exist (or not).

- **Options**:
  - ``--expect``: ``present`` (default) or ``absent`` to check for object
    presence.

The command prints a message for each missing or unexpected object as it is
found. The command exits with code ``1`` if any mismatches are detected. Otherwise it always exits with ``0``.

**Example**::

    pgq verify --expect present

``durability``
~~~~~~~~~~~~~~
Change the durability level of existing PGQueuer tables without data loss.

- **Arguments**:
  - ``durability`` (required): Desired durability mode (``volatile``, ``balanced``, or ``durable``).
  - ``--dry-run`` (optional): Print SQL commands without executing them.

**Example**::

    pgq durability durable

``autovac``
~~~~~~~~~~~
Apply recommended autovacuum settings for PGQueuer tables or roll back to defaults.

- **Options**:
  - ``--dry-run``: Print SQL commands without executing them.
  - ``--rollback``: Reset the settings to system defaults.

**Example**::

    pgq autovac

**Rollback Example**::

    pgq autovac --rollback

``queue``
~~~~~~~~~
Manually enqueue a job into the PGQueuer system.

- **Arguments**:
  - ``entrypoint`` (required): The entry point of the job.
  - ``payload`` (optional): A serialized string or JSON payload for the job.

**Example**::

    pgq queue my_module.my_function '{"key": "value"}'

``dashboard``
~~~~~~~~~~~~~
Display a live dashboard showing job statistics.

``listen``
~~~~~~~~~~
Listen to PostgreSQL NOTIFY channels for debugging.

``run``
~~~~~~~
Start a ``QueueManager`` to manage job queues and processes.

- **Options**:
  - ``--dequeue-timeout`` (float, default=30.0):
    Maximum number of seconds to wait for new jobs before returning an empty batch.
  - ``--batch-size`` (int, default=10):
    Number of jobs to dequeue and process in each batch.
  - ``--restart-delay`` (float, default=5.0):
    Delay in seconds between restarts if --restart-on-failure is used.
  - ``--restart-on-failure`` (boolean, default=False):
    Automatically restart the manager upon unexpected failure.
  - ``--log-level`` (str, default="INFO"):
    Logging level for pgqueuer output (DEBUG, INFO, WARNING, ERROR).
  - ``--mode`` (continuous|drain, default=continuous):
    Whether to run continuously or shut down once the queue is empty.
  - ``--max-concurrent-tasks`` (int|None, default=None):
    Limit the total number of tasks that can run at the same time. If unspecified or None, there is no limit.
  - ``--shutdown-on-listener-failure`` (bool, default = False):
    NEW. Shutdown the manager if the listener fails its periodic health-check probes.

This command initializes a job manager that continuously (or until drained) pulls tasks from the queue and runs them with worker processes. Use the ``--max-concurrent-tasks`` flag to cap the total concurrent tasks, thereby controlling resource usage to prevent excessive load.

**Example**::

    # Run with a limit of 5 concurrent tasks
    pgq run my_module:my_factory --max-concurrent-tasks 5

``schedules``
~~~~~~~~~~~~~
Manage schedules within PGQueuer. Use this command to display all schedules or remove specific ones by ID or name.

Why Use the ``run`` Option?
---------------------------

The ``run`` option is essential for setting up a reliable job processing environment. It initializes a ``QueueManager`` instance using a user-specified factory function while automatically handling system signals for graceful shutdowns.

When invoked, the ``run`` command:
- Registers termination signal handlers (e.g., SIGINT, SIGTERM).
- Ensures ongoing jobs are either completed or halted properly during shutdown.

This design minimizes disruptions and ensures job integrity.

**Usage Examples**::

    # Using the new alias
    pgq run <module+factory-function>

    # Using the traditional approach
    python3 -m pgqueuer run <module+factory-function>

### Queue Execution Modes

The `run` command supports two execution modes:

- **Continuous (default)**: Keeps processing jobs indefinitely, waiting for new ones as they arrive.
- **Drain**: Processes all available jobs and shuts down once the queue is empty.

**Example**:
```sh
pgq run my_module:my_factory --mode drain
```

Use **continuous** for long-running workers and **drain** for batch processing.

Durability Explained
--------------------

Durability determines the logging behavior of PGQueuer tables, affecting performance and data safety. PGQueuer offers three durability levels:

**Volatile**
~~~~~~~~~~~~
- **Description**: All tables are unlogged for maximum performance.
- **Behavior**: No data is written to the PostgreSQL Write-Ahead Log (WAL). Data is lost if the database crashes.
- **Use Case**: Suitable for temporary workloads where data loss is acceptable.

**Balanced**
~~~~~~~~~~~~
- **Description**: A middle ground between performance and durability.
- **Behavior**:
  - Critical tables (e.g., ``pgqueuer`` and ``pgqueuer_schedules``) are logged.
  - Auxiliary tables (e.g., ``pgqueuer_log`` and ``pgqueuer_statistics``) are unlogged.
- **Use Case**: Suitable when critical data must be durable, but non-critical logs and statistics can sacrifice durability for speed.

**Durable (Default)**
~~~~~~~~~~~~~~~~~~~~~
- **Description**: All tables are logged for maximum durability.
- **Behavior**: Data is written to WAL, providing full recovery in case of crashes.
- **Use Case**: Ideal for production environments where data integrity is critical.

Choosing a durability level involves trade-offs between performance and data safety. The ``volatile`` level maximizes performance but risks data loss during crashes. The ``balanced`` level offers a compromise, with critical data protected while auxiliary data is optimized for speed. The ``durable`` level ensures full data safety at the expense of performance.

Factory Call Cycle
------------------

The ``run`` command uses a factory pattern to instantiate and manage queue processors.
This section explains how your factory function integrates with PGQueuer's execution stack.

What is a Factory?
~~~~~~~~~~~~~~~~~~

A factory is a function you write that creates and configures a manager instance
(``PgQueuer``, ``QueueManager``, or ``SchedulerManager``). The factory is responsible for:

- Establishing the database connection
- Creating the manager instance
- Registering your entrypoints and schedules
- Optionally setting up shared resources

The CLI loads your factory, calls it, and runs the returned manager until shutdown.

Execution Flow
~~~~~~~~~~~~~~

When you execute ``pgq run my_module:create_pgqueuer``, PGQueuer performs these steps:

::

    ┌─────────────────────────────────────────────────────────────────────────┐
    │                     pgq run my_module:factory                           │
    └─────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
    ┌─────────────────────────────────────────────────────────────────────────┐
    │ 1. LOAD FACTORY                                                         │
    │    Parse the factory path and import your function. If you pass a       │
    │    callable directly (lambda, partial), it is used as-is.               │
    └─────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
    ┌─────────────────────────────────────────────────────────────────────────┐
    │ 2. SETUP SIGNAL HANDLERS                                                │
    │    Register handlers for SIGINT and SIGTERM so the process can          │
    │    shut down gracefully when interrupted.                               │
    └─────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
    ┌─────────────────────────────────────────────────────────────────────────┐
    │ 3. SUPERVISOR LOOP                                                      │
    │    Enter a loop that continues until shutdown is signaled.              │
    │    This loop enables automatic restarts if configured.                  │
    └─────────────────────────────────────────────────────────────────────────┘
                                       │
                          ┌────────────┴────────────┐
                          │      MAIN LOOP          │
                          └────────────┬────────────┘
                                       ▼
    ┌─────────────────────────────────────────────────────────────────────────┐
    │ 4. INVOKE YOUR FACTORY                                                  │
    │    Your factory function runs, creating the database connection,        │
    │    registering entrypoints, and returning a configured manager.         │
    │                                                                         │
    │    The result is wrapped uniformly regardless of whether you return     │
    │    a simple value, a context manager, or an async context manager.      │
    └─────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
    ┌─────────────────────────────────────────────────────────────────────────┐
    │ 5. LINK SHUTDOWN EVENT                                                  │
    │    The supervisor connects its shutdown event to the manager so         │
    │    that signals propagate correctly.                                    │
    └─────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
    ┌─────────────────────────────────────────────────────────────────────────┐
    │ 6. RUN THE MANAGER                                                      │
    │    The manager's run() method starts processing:                        │
    │      • QueueManager fetches and dispatches jobs                         │
    │      • SchedulerManager executes cron-scheduled tasks                   │
    │      • PgQueuer runs both concurrently                                  │
    └─────────────────────────────────────────────────────────────────────────┘
                                       │
                          ┌────────────┴────────────┐
                          ▼                         ▼
    ┌──────────────────────────────┐  ┌──────────────────────────────────────┐
    │ 7a. GRACEFUL SHUTDOWN        │  │ 7b. RESTART ON FAILURE               │
    │  Signal received (Ctrl+C)    │  │  Exception caught during execution   │
    │  ↓                           │  │  ↓                                   │
    │  Complete in-flight jobs     │  │  Wait the configured restart delay   │
    │  ↓                           │  │  ↓                                   │
    │  Run context manager cleanup │  │  Loop back to step 4 (re-invoke      │
    │  ↓                           │  │  your factory fresh)                 │
    │  Exit process                │  │                                      │
    └──────────────────────────────┘  └──────────────────────────────────────┘

Factory Input Types
~~~~~~~~~~~~~~~~~~~

The factory loader accepts two input types:

**String path** (used with CLI):
  A module path in the format ``module:function``. The loader imports the module
  and retrieves the function. Example: ``my_app.workers:create_pgqueuer``

**Callable** (used programmatically):
  When calling ``run()`` directly from Python, you can pass a callable such as
  a lambda or ``functools.partial``. This enables dynamic configuration without
  needing a separate module. See ``examples/callable_factory/`` for this pattern.

Factory Return Types
~~~~~~~~~~~~~~~~~~~~

Your factory can return the manager in three ways, and PGQueuer handles each uniformly:

**Simple return value**:
  An async function that returns the manager directly. Simple to write but
  offers no cleanup hook when the manager shuts down.

**Async context manager**:
  Uses ``@asynccontextmanager`` to yield the manager. Code before ``yield`` runs
  at startup; code after ``yield`` (in ``finally``) runs at shutdown. This is the
  recommended pattern when you need to close connections or release resources.

**Sync context manager**:
  Same as above but synchronous. Useful when wrapping libraries that require
  synchronous cleanup.

The supervisor wraps all three types into a unified async context manager internally,
so you can choose whichever pattern fits your use case.

Restart Behavior
~~~~~~~~~~~~~~~~

When ``--restart-on-failure`` is enabled:

1. If an exception occurs during manager execution, it is logged
2. The supervisor waits for the configured ``--restart-delay``
3. Your factory is called again fresh, creating new connections and state
4. The manager runs again from the beginning

This means your factory should be idempotent—safe to call multiple times.
Each restart gets a completely fresh manager instance.

Shutdown Behavior
~~~~~~~~~~~~~~~~~

When a shutdown signal is received (SIGINT or SIGTERM):

1. The shutdown event is set, signaling the manager to stop
2. The manager finishes processing any in-flight jobs
3. If you used a context manager factory, the cleanup code runs
4. The process exits cleanly

This ensures jobs are not abandoned mid-execution and resources are properly released.

Shared Resources
~~~~~~~~~~~~~~~~

Your factory can initialize expensive resources once (HTTP clients, connection pools,
ML models) and pass them to the manager via the ``resources`` parameter. These are
then accessible in your entrypoint functions through the job context.

Resources are:

- Created once when the factory runs
- Shared across all job executions within that manager instance
- Re-created on restart (since the factory runs again)
- Cleaned up via context manager teardown

See ``examples/consumer.py`` for a working example of shared resources.

Key Points
~~~~~~~~~~

- **Factory runs on each restart**: With ``--restart-on-failure``, your factory
  executes again after failures, creating fresh connections and state.

- **Context managers enable cleanup**: Use async context managers when you need
  to close connections or release resources on shutdown.

- **Callables enable dynamic configuration**: Pass lambdas or partials when you
  need runtime parameters without creating separate module files.

- **Shutdown is graceful**: In-flight jobs complete before teardown runs,
  preventing abandoned work.

See Also
~~~~~~~~

- ``examples/consumer.py`` — Async context manager factory pattern
- ``examples/callable_factory/`` — Callable factory with custom CLI and parameters
