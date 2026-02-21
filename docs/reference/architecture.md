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
%%{init: {'flowchart': {'htmlLabels': true, 'curve': 'linear', 'padding': '10'}, 'theme': 'base', 'themeVariables': {'fontSize': '16px', 'fontFamily': 'Inter, sans-serif'}}}%%
flowchart LR
    Wait["<b>WAIT</b><br/>for NOTIFY"]
    Query["<b>QUERY</b><br/>jobs"]
    Found{{"<b>Found?</b>"}}
    Claim["<b>CLAIM</b><br/>picked"]
    Execute["<b>EXEC</b><br/>task"]
    Result{{"<b>OK?</b>"}}
    Success["<b>SUCCESS</b>"]
    Error["<b>ERROR</b>"]

    Wait --> Query
    Query --> Found
    Found -->|YES| Claim
    Found -->|NO| Wait
    Claim --> Execute
    Execute --> Result
    Result -->|YES| Success
    Result -->|NO| Error
    Success --> Wait
    Error --> Wait

    classDef wait fill:#6B8FC7,stroke:#4A6FA5,stroke-width:2px,color:#fff
    classDef query fill:#2E5080,stroke:#1a2f40,stroke-width:2px,color:#fff
    classDef process fill:#4A6FA5,stroke:#2E5080,stroke-width:2px,color:#fff
    classDef success fill:#2D9D78,stroke:#1d6d55,stroke-width:2px,color:#fff
    classDef error fill:#C1666B,stroke:#8b3a3f,stroke-width:2px,color:#fff
    classDef decision fill:#D4A240,stroke:#8b6e1a,stroke-width:2px,color:#000

    class Wait wait
    class Query query
    class Found,Result decision
    class Claim,Execute process
    class Success success
    class Error error
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
%%{init: {'flowchart': {'htmlLabels': true, 'curve': 'linear', 'padding': '10'}, 'theme': 'base', 'themeVariables': {'fontSize': '16px', 'fontFamily': 'Inter, sans-serif'}}}%%
flowchart LR
    Queued["<b>QUEUED</b>"]
    Picked["<b>PICKED</b>"]
    Success["<b>SUCCESS</b>"]
    Exception["<b>EXCEPTION</b>"]
    Canceled["<b>CANCELED</b>"]
    Deleted["<b>DELETED</b>"]

    Queued -->|claim| Picked
    Queued -->|delete| Deleted
    Picked -->|success| Success
    Picked -->|error| Exception
    Picked -->|cancel| Canceled

    classDef queued fill:#6B8FC7,stroke:#4A6FA5,stroke-width:2px,color:#fff
    classDef picked fill:#4A6FA5,stroke:#2E5080,stroke-width:2px,color:#fff
    classDef success fill:#2D9D78,stroke:#1d6d55,stroke-width:2px,color:#fff
    classDef error fill:#C1666B,stroke:#8b3a3f,stroke-width:2px,color:#fff
    classDef canceled fill:#D4A240,stroke:#8b6e1a,stroke-width:2px,color:#000

    class Queued queued
    class Picked picked
    class Success success
    class Exception,Deleted error
    class Canceled canceled
```
