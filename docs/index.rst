Welcome to PgQueuer's documentation!
====================================

.. figure:: logo.png
   :alt: Logo
   :width: 500
   :align: center


Introduction
============
PgQueuer is a minimalist, high-performance job queue library for Python, leveraging the robustness of PostgreSQL. Designed for simplicity and efficiency, PgQueuer uses PostgreSQL's LISTEN/NOTIFY to manage job queues effortlessly.

The repository is hosted on `github <https://github.com/janbjorge/PgQueuer>`_

Installation
============

Install PgQueuer using pip:

.. code-block:: bash

    pip install PgQueuer


Features
========

- **Simple Integration**: Easily integrate with existing Python applications using PostgreSQL.
- **Efficient Concurrency Handling**: Utilizes PostgreSQL's `FOR UPDATE SKIP LOCKED` for reliable and concurrent job processing.
- **Real-time Notifications**: Leverages `LISTEN` and `NOTIFY` for real-time updates on job status changes.
- **Batch Processing**: Handles large job batches efficiently for both enqueueing and dequeueing.

Example Usage
=============

The following example demonstrates how to set up a PostgreSQL event queue in PGCacheWatch, connect to a PostgreSQL channel, and listen for events:

.. code-block:: python
   import asyncio

   import asyncpg
   from PgQueuer.db import AsyncpgDriver
   from PgQueuer.models import Job
   from PgQueuer.qm import QueueManager


   async def main() -> None:
      connection = await asyncpg.connect()
      driver = AsyncpgDriver(connection)
      qm = QueueManager(driver)

      # Setup the 'fetch' entrypoint
      @qm.entrypoint("fetch")
      async def process_message(job: Job) -> None:
         print(f"Processed message: {job}")

      N = 1_000
      # Enqueue jobs.
      await qm.queries.enqueue(
         ["fetch"] * N,
         [f"this is from me: {n}".encode() for n in range(N)],
         [0] * N,
      )

      await qm.run()


   if __name__ == "__main__":
      asyncio.run(main())

.. toctree::
   :maxdepth: 2

   database_initialization
   queuemanager
   dashboard
   benchmark
