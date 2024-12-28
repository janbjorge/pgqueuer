Welcome to PGQueuer's documentation!
====================================

.. figure:: logo.png
   :alt: Logo
   :width: 500
   :align: center


Introduction
============
PGQueuer is a minimalist, high-performance job queue library for Python, leveraging the robustness of PostgreSQL. Designed for simplicity and efficiency, PGQueuer uses PostgreSQL's LISTEN/NOTIFY to manage job queues effortlessly.

The repository is hosted on `github <https://github.com/janbjorge/PGQueuer>`_

Installation
------------

Install PGQueuer using pip:

.. code-block:: bash

    pip install pgqueuer


Features
--------

- **Simple Integration**: Easily integrate with existing Python applications using PostgreSQL.
- **Efficient Concurrency Handling**: Utilizes PostgreSQL's `FOR UPDATE SKIP LOCKED` for reliable and concurrent job processing.
- **Real-time Notifications**: Leverages `LISTEN` and `NOTIFY` for real-time updates on job status changes.
- **Batch Processing**: Handles large job batches efficiently for both enqueueing and dequeueing.

Example Usage
-------------

Here's how you can use PGQueuer in a typical scenario processing incoming data messages:

Start a consumer
~~~~~~~~~~~~~~~~

Start a long-lived consumer that will begin processing jobs as soon as they are enqueued by another process.

.. code-block:: bash

    pgq run examples.consumer.main

Start a producer
~~~~~~~~~~~~~~~~

Start a short-lived producer that will enqueue 10,000 jobs.

.. code-block:: bash

    python3 examples/producer.py 10000

.. toctree::
   :maxdepth: 2

   cli
   database_initialization
   pgqueuer
   driver
   dashboard
   benchmark
   prometheus-metrics-service
