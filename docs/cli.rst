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

``alter-durability``
~~~~~~~~~~~~~~~~~~~~~
Change the durability level of existing PGQueuer tables without data loss.

- **Arguments**:
  - ``durability`` (required): Desired durability mode (``volatile``, ``balanced``, or ``durable``).
  - ``--dry-run`` (optional): Print SQL commands without executing them.

**Example**::

    pgq alter-durability durable

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
  - ``--shutdown-on-listener-failure`` (bool, default = False):
    NEW. Shutdown the manager if the listener fails its periodic health‑check probes.

This command initializes a job manager that continuously (or until drained) pulls tasks from the queue and runs them with worker processes. Use the ``--max-concurrent-tasks`` flag to cap the total concurrent tasks, thereby controlling resource usage to prevent excessive load.

**Example**::

    # Run with a limit of 5 concurrent tasks
    pgq run my_module.my_factory --max-concurrent-tasks 5

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
pgq run my_module.my_factory --mode drain
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
