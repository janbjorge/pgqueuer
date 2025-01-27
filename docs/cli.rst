CLI Module for PGQueuer
=======================

The pgq CLI provides a command-line interface for managing various aspects of the PGQueuer system. The CLI can be invoked via the alias `pgq` or the traditional method `python3 -m pgqueuer`. Both methods are fully supported and provide identical functionality.

Functionality
-------------

The CLI offers several commands to install, uninstall, upgrade, and manage the job queue system. Additionally, it allows for real-time monitoring through a dashboard and specific PostgreSQL NOTIFY channels.

Key Commands
------------

- ``install``: Set up the necessary database schema for PGQueuer.
    - Supports the ``--durability`` option to define the durability level for tables. Possible values are:
        - ``volatile``: All tables are unlogged, prioritizing maximum performance over durability.
        - ``balanced``: Critical tables (e.g., `pgqueuer` and `pgqueuer_schedules`) are logged, while auxiliary tables are unlogged for improved performance.
        - ``durable``: All tables are logged to ensure maximum durability.
        - **Default**: ``durable``.

    Example:

    ```bash
    pgq install --durability balanced
    ```

- ``uninstall``: Remove the PGQueuer schema from the database.

- ``upgrade``: Apply database schema upgrades to PGQueuer.
    - Also supports the ``--durability`` option, similar to ``install``, allowing you to adjust the durability level during the upgrade process.

    Example:

    ```bash
    pgq upgrade --durability durable
    ```

- ``alter-durability``: Change the durability level of existing PGQueuer tables without data loss.
    - Arguments:
        - ``durability`` (required): The desired durability mode (`volatile`, `balanced`, or `durable`).
        - ``--dry-run`` (optional): Print SQL commands without executing them.

    Example:

    ```bash
    pgq alter-durability durable
    ```

- ``queue``: Manually enqueue a job into the PGQueuer system.
    - Arguments:
        - ``entrypoint`` (required): The entry point of the job to be executed.
        - ``payload`` (optional): A serialized string or JSON payload for the job.

    Example:

    ```bash
    pgq queue my_module.my_function '{"key": "value"}'
    ```

- ``dashboard``: Display a live dashboard showing job statistics.

- ``listen``: Listen to PostgreSQL NOTIFY channels for debugging.

- ``run``: Start a QueueManager that manages job queues and processes.

- ``schedules``: Manage schedules within the PGQueuer system. You can display all schedules or remove specific ones by ID or name.

Why Use the ``run`` Option
--------------------------

The ``run`` option is particularly important because it encapsulates the creation and management of a `QueueManager` instance from a user-specified factory function. This command is crucial for setting up a reliable job processing environment as it automatically handles signal setup for graceful shutdowns.

When the `run` command is invoked, it ensures that appropriate signal handlers are registered. These handlers listen for termination signals (like SIGINT or SIGTERM), enabling the `QueueManager` to shut down gracefully. This setup prevents job processing disruptions and ensures that all jobs are either completed or properly halted when the application receives a shutdown signal.

Usage
-----

To use the CLI, invoke it with the desired command and options. You can use either the new alias `pgq` or the original command structure. Here are examples of both methods:

.. code-block:: bash

    # Using the new alias
    pgq run <module+factory-function>

    # Using the traditional approach
    python3 -m pgqueuer run <module+factory-function>

This command initializes the QueueManager using the factory function provided, setting up signal handling automatically to manage job processing interruptions gracefully.

The new `pgq` alias makes it more convenient to work with the CLI, while maintaining full compatibility with the traditional approach for those who prefer it.

Durability Explained
--------------------

Durability in PGQueuer determines the logging behavior of the database tables, which directly affects performance and data safety. PGQueuer offers three durability levels:

- **volatile**:
  - All tables are unlogged, prioritizing maximum performance.
  - Data in unlogged tables is not written to the PostgreSQL Write-Ahead Log (WAL), making operations faster but at the cost of data safety.
  - If the database crashes, all data in unlogged tables is lost.
  - Suitable for temporary or ephemeral workloads where data loss is acceptable.

- **balanced**:
  - Critical tables (e.g., `pgqueuer` and `pgqueuer_schedules`) are logged to ensure durability, while auxiliary tables (e.g., `pgqueuer_log` and `pgqueuer_statistics`) are unlogged for improved performance.
  - This provides a middle ground between performance and data safety.
  - Suitable for use cases where durability is important for critical data, but non-critical logs and statistics can be sacrificed for performance.

- **durable** (default):
  - All tables are logged, ensuring maximum data durability.
  - Data is written to the WAL, providing full recovery in case of a crash.
  - This is ideal for production environments where data integrity is critical, but it comes with a performance cost due to the overhead of logging.

Choosing a durability level involves trade-offs between performance and data safety. The **volatile** level maximizes performance but risks data loss during crashes. The **balanced** level offers a compromise, with critical data protected while auxiliary data is optimized for speed. The **durable** level ensures full data safety at the expense of performance.