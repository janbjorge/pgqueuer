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
%%{init: {'flowchart': {'htmlLabels': true, 'curve': 'linear'}, 'theme': 'base', 'themeVariables': {'primaryColor':'#fff', 'primaryTextColor':'#000', 'primaryBorderColor':'#000', 'fontSize': '28px', 'fontFamily': 'Inter, sans-serif'}}}%%
flowchart TD
    A["<b style='font-size:28px'>WAIT</b><br/>for NOTIFY"]
    B["<b style='font-size:28px'>QUERY</b><br/>queued jobs"]
    C{{"<b style='font-size:26px'>Found?</b>"}}
    D["<b style='font-size:28px'>CLAIM</b><br/>→ picked"]
    E["<b style='font-size:28px'>EXECUTE</b><br/>entrypoint"]
    F{{"<b style='font-size:26px'>Success?</b>"}}
    G["<b style='font-size:28px'>MARK</b><br/>successful"]
    H["<b style='font-size:28px'>MARK</b><br/>exception"]

    A --> B
    B --> C
    C -->|"YES"| D
    C -->|"NO"| A
    D --> E
    E --> F
    F -->|"YES"| G
    F -->|"NO"| H
    G --> A
    H --> A

    classDef wait fill:#6B8FC7,stroke:#4A6FA5,stroke-width:3px,color:#fff
    classDef query fill:#2E5080,stroke:#1a2f40,stroke-width:3px,color:#fff
    classDef process fill:#4A6FA5,stroke:#2E5080,stroke-width:3px,color:#fff
    classDef success fill:#2D9D78,stroke:#1d6d55,stroke-width:3px,color:#fff
    classDef error fill:#C1666B,stroke:#8b3a3f,stroke-width:3px,color:#fff
    classDef decision fill:#D4A240,stroke:#8b6e1a,stroke-width:3px,color:#fff

    class A wait
    class B query
    class C,F decision
    class D,E process
    class G success
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
%%{init: {'flowchart': {'htmlLabels': true, 'curve': 'linear'}, 'theme': 'base', 'themeVariables': {'primaryColor':'#fff', 'primaryTextColor':'#000', 'primaryBorderColor':'#000', 'fontSize': '28px', 'fontFamily': 'Inter, sans-serif'}}}%%
flowchart LR
    Start(["START"]) --> Queued["<b style='font-size:28px'>QUEUED</b>"]
    Queued -->|"claim"| Picked["<b style='font-size:28px'>PICKED</b>"]
    Queued -->|"delete"| Deleted["<b style='font-size:28px'>DELETED</b>"]
    Picked -->|"success"| Success["<b style='font-size:28px'>SUCCESSFUL</b>"]
    Picked -->|"error"| Exception["<b style='font-size:28px'>EXCEPTION</b>"]
    Picked -->|"cancel"| Canceled["<b style='font-size:28px'>CANCELED</b>"]
    Success --> End(["END"])
    Exception --> End
    Canceled --> End
    Deleted --> End

    classDef queuedStyle fill:#6B8FC7,stroke:#4A6FA5,stroke-width:3px,color:#fff
    classDef pickedStyle fill:#4A6FA5,stroke:#2E5080,stroke-width:3px,color:#fff
    classDef successStyle fill:#2D9D78,stroke:#1d6d55,stroke-width:3px,color:#fff
    classDef exceptionStyle fill:#C1666B,stroke:#8b3a3f,stroke-width:3px,color:#fff
    classDef canceledStyle fill:#D4A240,stroke:#8b6e1a,stroke-width:3px,color:#fff
    classDef terminalStyle fill:#2E5080,stroke:#1a2f40,stroke-width:3px,color:#fff

    class Queued queuedStyle
    class Picked pickedStyle
    class Success successStyle
    class Exception exceptionStyle
    class Canceled canceledStyle
    class Start,End terminalStyle
```
