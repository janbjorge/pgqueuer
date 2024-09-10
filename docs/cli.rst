CLI Module for PGQueuer
=======================

The pgq cli provides a command-line interface for managing various aspects of the PGQueuer system.

Functionality
-------------

The CLI offers several commands to install, uninstall, upgrade, and manage the job queue system. Additionally, it allows for real-time monitoring through a dashboard and specific PostgreSQL NOTIFY channels.

Key Commands
------------

- ``install``: Set up the necessary database schema for PGQueuer.
- ``uninstall``: Remove the PGQueuer schema from the database.
- ``upgrade``: Apply database schema upgrades to PGQueuer.
- ``dashboard``: Display a live dashboard showing job statistics.
- ``listen``: Listen to PostgreSQL NOTIFY channels for debugging.
- ``run``: Start a QueueManager that manages job queues and processes.

Why Use the ``run`` Option
--------------------------

The ``run`` option is particularly important because it encapsulates the creation and management of a `QueueManager` instance from a user-specified factory function. This command is crucial for setting up a reliable job processing environment as it automatically handles signal setup for graceful shutdowns.

When the `run` command is invoked, it ensures that appropriate signal handlers are registered. These handlers listen for termination signals (like SIGINT or SIGTERM), enabling the `QueueManager` to shut down gracefully. This setup prevents job processing disruptions and ensures that all jobs are either completed or properly halted when the application receives a shutdown signal.

Usage
-----

To use the CLI, invoke it with the desired command and options. For example, to start the QueueManager with automatic signal handling:

.. code-block:: bash

    python3 -m pgqueuer run <moduele+factory-function>


This command initializes the QueueManager using the factory function provided, setting up signal handling automatically to manage job processing interruptions gracefully.
