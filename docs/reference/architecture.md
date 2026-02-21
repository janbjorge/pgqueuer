# Architecture

This overview explains how data flows from a producer to a consumer in PGQueuer.
It highlights how entrypoints route jobs and how PostgreSQL notifications keep
consumers up to date.

## Job Flow Diagram

```mermaid
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

## QueueManager Processing Loop

```mermaid
flowchart LR
    A["Wait for notification<br/>or timeout"] --> B["SELECT * FROM jobs<br/>WHERE status='queued'<br/>FOR UPDATE SKIP LOCKED"]
    B --> C{"Jobs<br/>available?"}
    C -->|Yes| D["Claim job<br/>status→'picked'"]
    C -->|No| A
    D --> E["Dispatch to<br/>@pgq.entrypoint"]
    E --> F{"Execution<br/>result?"}
    F -->|Success| G["Update status<br/>→ 'successful'"]
    F -->|Error| H["Update status<br/>→ 'exception'"]
    G --> A
    H --> A

    classDef wait fill:#4A6FA5,stroke:#2E5080,color:#fff,stroke-width:2px
    classDef query fill:#2E5080,stroke:#1a2f40,color:#fff,stroke-width:2px
    classDef process fill:#6B8FC7,stroke:#4A6FA5,color:#fff,stroke-width:2px
    classDef result fill:#28a745,stroke:#1a5e1a,color:#fff,stroke-width:2px
    classDef error fill:#dc3545,stroke:#8b0000,color:#fff,stroke-width:2px

    class A wait
    class B,C query
    class D,E process
    class G result
    class H error
```

## Job Status Lifecycle

PGQueuer tracks each job's progress using a dedicated PostgreSQL ENUM type,
`pgqueuer_status` by default:

```sql
CREATE TYPE pgqueuer_status AS ENUM (
    'queued',
    'picked',
    'successful',
    'exception',
    'canceled',
    'deleted'
);
```

The lifecycle of a job flows through these statuses:

- **`queued`** — Newly enqueued jobs start here and wait for a worker to pick them up.
- **`picked`** — Set by `QueueManager` when a worker begins processing. A heartbeat
  timestamp tracks active work.
- **`successful`** — Assigned after a job completes without errors. Details are copied to
  the statistics log and removed from the queue.
- **`exception`** — Indicates the job failed with an uncaught error. The traceback is
  stored for later inspection.
- **`canceled`** — Jobs canceled before completion receive this status and are logged.
- **`deleted`** — Used when jobs are removed from the queue without running, such as during
  manual cleanup operations.

### Status Transition Diagram

```mermaid
stateDiagram-v2
    direction LR

    [*] --> queued

    queued --> picked: QueueManager claims
    queued --> deleted: Manual cleanup

    picked --> successful: Execution completes
    picked --> exception: Uncaught error
    picked --> canceled: Cancellation triggered

    successful --> [*]
    exception --> [*]
    canceled --> [*]
    deleted --> [*]

    classDef pending fill:#4A6FA5,stroke:#2E5080,color:#fff,stroke-width:2px
    classDef processing fill:#6B8FC7,stroke:#4A6FA5,color:#fff,stroke-width:2px
    classDef success fill:#28a745,stroke:#1a5e1a,color:#fff,stroke-width:2px
    classDef failure fill:#dc3545,stroke:#8b0000,color:#fff,stroke-width:2px
    classDef canceled fill:#ffc107,stroke:#8b6f00,color:#000,stroke-width:2px

    class queued pending
    class picked processing
    class successful success
    class exception failure
    class canceled canceled
    class deleted failure
```
