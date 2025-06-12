# Architecture

This overview explains how data flows from a producer to a consumer in PGQueuer.
It highlights how entrypoints route jobs and how PostgreSQL notifications keep
consumers up to date.

## Job Flow Diagram

```{mermaid}
flowchart LR
    P[Producer]
    DB[(PostgreSQL)]
    R[EventRouter]
    QM[QueueManager]
    C[Consumer]

    P -- "Queries.enqueue" --> DB
    DB -- "NOTIFY table_changed" --> R
    R -- "signal pending work" --> QM
    QM -- "dispatch job" --> C
    C -- "update status" --> DB
```

1. **Producer** inserts a job using `Queries.enqueue()`.
2. A trigger emits a `table_changed_event` via **NOTIFY** on the configured channel.
3. The **EventRouter** places the event in a `PGNoticeEventListener` queue.
4. The **QueueManager** waits for events, fetches ready jobs with `FOR UPDATE SKIP LOCKED`, and dispatches them to registered entrypoints.
5. After execution, the **Consumer** updates job status back in PostgreSQL.

Endpoint routing is handled by `EventRouter` which maps notification types to
functions registered via `@pgq.entrypoint`. Notifications delivered through
`LISTEN/NOTIFY` ensure consumers promptly react to new work.
