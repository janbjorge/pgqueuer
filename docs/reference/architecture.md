# Architecture

This overview explains how data flows from a producer to a consumer in PGQueuer.
It highlights how entrypoints route jobs and how PostgreSQL notifications keep
consumers up to date.

## Job Flow Diagram

```mermaid
%%{init: {'theme': 'base', 'themeVariables': {'fontSize': '14px', 'fontFamily': 'Inter, sans-serif'}}}%%
flowchart LR
    P[Producer]
    DB[(PostgreSQL)]
    R[EventRouter]
    QM[QueueManager]
    C[Consumer]

    P -->|Queries.enqueue| DB
    DB -->|NOTIFY| R
    R -->|signal| QM
    QM -->|dispatch| C
    C -->|update status| DB

    classDef producer fill:#DDEAF7,stroke:#4A6FA5,stroke-width:2px,color:#111
    classDef database fill:#D0DCF0,stroke:#2E5080,stroke-width:2px,color:#111
    classDef router   fill:#DDEAF7,stroke:#4A6FA5,stroke-width:2px,color:#111
    classDef consumer fill:#D5EDE5,stroke:#2D9D78,stroke-width:2px,color:#111

    class P producer
    class DB database
    class R,QM router
    class C consumer
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
%%{init: {'theme': 'base', 'themeVariables': {'fontSize': '14px', 'fontFamily': 'Inter, sans-serif'}}}%%
flowchart LR
    Wait[Wait for NOTIFY] --> Query[Query jobs]
    Query -->|no jobs| Wait
    Query -->|jobs found| Claim[Claim job]
    Claim --> Execute[Execute task]
    Execute -->|success| Success[Mark successful]
    Execute -->|error| Error[Mark exception]
    Success --> Wait
    Error --> Wait

    classDef wait    fill:#DDEAF7,stroke:#4A6FA5,stroke-width:2px,color:#111
    classDef db      fill:#D0DCF0,stroke:#2E5080,stroke-width:2px,color:#111
    classDef process fill:#DDEAF7,stroke:#4A6FA5,stroke-width:2px,color:#111
    classDef success fill:#D5EDE5,stroke:#2D9D78,stroke-width:2px,color:#111
    classDef error   fill:#F5DADA,stroke:#C1666B,stroke-width:2px,color:#111

    class Wait wait
    class Query db
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
%%{init: {'theme': 'base', 'themeVariables': {'fontSize': '14px', 'fontFamily': 'Inter, sans-serif'}}}%%
flowchart LR
    Queued[queued]
    Picked[picked]
    Success[successful]
    Exception[exception]
    Canceled[canceled]
    Deleted[deleted]

    Queued -->|claim| Picked
    Queued -->|delete| Deleted
    Picked -->|success| Success
    Picked -->|error| Exception
    Picked -->|cancel| Canceled

    classDef queued   fill:#DDEAF7,stroke:#4A6FA5,stroke-width:2px,color:#111
    classDef picked   fill:#D0DCF0,stroke:#2E5080,stroke-width:2px,color:#111
    classDef success  fill:#D5EDE5,stroke:#2D9D78,stroke-width:2px,color:#111
    classDef error    fill:#F5DADA,stroke:#C1666B,stroke-width:2px,color:#111
    classDef canceled fill:#FBF0D5,stroke:#D4A240,stroke-width:2px,color:#111

    class Queued queued
    class Picked picked
    class Success success
    class Exception,Deleted error
    class Canceled canceled
```
