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
------------

Install PgQueuer using pip:

.. code-block:: bash

    pip install PgQueuer


Features
--------

- **Simple Integration**: Easily integrate with existing Python applications using PostgreSQL.
- **Efficient Concurrency Handling**: Utilizes PostgreSQL's `FOR UPDATE SKIP LOCKED` for reliable and concurrent job processing.
- **Real-time Notifications**: Leverages `LISTEN` and `NOTIFY` for real-time updates on job status changes.
- **Batch Processing**: Handles large job batches efficiently for both enqueueing and dequeueing.

Example Usage
-------------

Here's how you can use PgQueuer in a typical scenario processing incoming data messages:

Start a consumer
~~~~~~~~~~~~~~~~

Start a long-lived consumer that will begin processing jobs as soon as they are enqueued by another process.

.. code-block:: bash

    python3 -m PgQueuer run tools.consumer.main

Start a producer
~~~~~~~~~~~~~~~~

Start a short-lived producer that will enqueue 10,000 jobs.

.. code-block:: bash

    python3 tools/producer.py 10000

.. toctree::
   :maxdepth: 2

   cli
   database_initialization
   queuemanager
   rps
   dashboard
   benchmark
