Welcome to PGQueuer's Documentation
====================================

.. figure:: logo.png
   :alt: Logo
   :width: 500
   :align: center

Introduction
============
PGQueuer is a minimalist, high-performance job queue library for Python, leveraging the robustness of PostgreSQL. Designed for simplicity and efficiency, PGQueuer uses PostgreSQL's LISTEN/NOTIFY to manage job queues effortlessly.

The repository is hosted on `GitHub <https://github.com/janbjorge/PGQueuer>`_.

Installation
------------

Install PGQueuer using pip:

.. code-block:: bash

    pip install PGQueuer

Features
--------

- **Simple Integration**: Easily integrate with existing Python applications using PostgreSQL.
- **Efficient Concurrency Handling**: Utilizes PostgreSQL's `FOR UPDATE SKIP LOCKED` for reliable and concurrent job processing.
- **Real-time Notifications**: Leverages `LISTEN` and `NOTIFY` for real-time updates on job status changes.
- **Batch Processing**: Handles large job batches efficiently for both enqueueing and dequeueing.

Use Case Examples
-----------------

### Example 1: Real-time Data Processing

Imagine you are building a system that processes incoming messages from users in real-time. PGQueuer allows you to easily queue these messages and process them in the order they are received, while prioritizing certain types of messages.

- **Producer**: Enqueues messages as they arrive from users.
- **Consumer**: Processes messages, prioritizing urgent ones first.

Start a producer to enqueue messages:

.. code-block:: bash

    python3 tools/producer.py 10000

Then start a consumer to process them:

.. code-block:: bash

    python3 -m PGQueuer run tools.consumer.main

Here is the source code for the producer:

.. code-block:: python

    from __future__ import annotations

    import asyncio
    import sys

    import asyncpg

    from pgqueuer.db import AsyncpgDriver
    from pgqueuer.queries import Queries

    async def main(N: int) -> None:
        connection = await asyncpg.connect()
        driver = AsyncpgDriver(connection)
        queries = Queries(driver)
        await queries.enqueue(
            ["fetch"] * N,
            [f"this is from me: {n}".encode() for n in range(1, N + 1)],
            [0] * N,
        )

    if __name__ == "__main__":
        N = 1_000 if len(sys.argv) == 1 else int(sys.argv[1])
        asyncio.run(main(N))

And here is the source code for the consumer:

.. code-block:: python

    from __future__ import annotations

    import asyncio
    import sys

    import asyncpg

    from pgqueuer.db import AsyncpgDriver
    from pgqueuer.queries import Queries

    async def main(N: int) -> None:
        connection = await asyncpg.connect()
        driver = AsyncpgDriver(connection)
        queries = Queries(driver)
        await queries.enqueue(
            ["fetch"] * N,
            [f"this is from me: {n}".encode() for n in range(1, N + 1)],
            [0] * N,
        )

    if __name__ == "__main__":
        N = 1_000 if len(sys.argv) == 1 else int(sys.argv[1])
        asyncio.run(main(N))

### Example 2: Sequential Task Processing

In certain workflows, you may need to execute a sequence of tasks where each task depends on the output of the previous one. With PGQueuer, you can easily model such workflows by chaining job processing between different entrypoints.

- **Step 1**: A producer adds multiple data preprocessing jobs to the queue.
- **Step 2**: Once the preprocessing is done, the consumer enqueues the next set of jobs for deeper analysis.
- **Step 3**: Results from the analysis are then used to enqueue final jobs for generating reports.

Each step runs in sequence, with PGQueuer managing the transitions smoothly by having consumers enqueue new tasks upon completion of previous ones. This ensures that your workflow tasks are processed in the correct order without manual intervention.

Example to enqueue the next task after completion:

.. code-block:: python

    async def process_and_enqueue_next_task(job):
        # Process the current job
        await process_job(job)

        # Enqueue the next job in the workflow
        await queries.enqueue(["analyze"], [b"data to analyze"], [1])

Explore the Documentation
-------------------------
Whether you are building a small automation script or scaling a large system, PGQueuer's documentation will guide you through each feature and concept.

.. toctree::
   :maxdepth: 2

   cli
   database_initialization
   queuemanager
   limits
   heartbeat
   dashboard
   benchmark
   prometheus-metrics-service

For more examples and advanced use cases, see the individual sections and examples within each topic.

