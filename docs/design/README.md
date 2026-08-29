# PgQueuer system design

This document is the top-level model of PgQueuer. It names the
participants, the use cases, the domain objects, and the state machines, and
it links out to the records and reference pages that own the details. When a
sub-area grows past what a section here can hold, it gets its own model
document and this one keeps the summary plus a link.

Related documents:

- [Architecture Decision Records](../adr/README.md) cover *why* the system
  is shaped this way. This document describes *what is*; every "why" belongs
  in an ADR. When an edit here introduces a new "why", stop and write the
  ADR instead.
- The [architecture reference](../reference/architecture.md) shows the job
  flow and the job status state machine in end-user terms.
- [Ports & Adapters](../reference/ports-and-adapters.md) describes the
  layering that implements this model.

## Overview

PgQueuer turns a PostgreSQL database the user already operates into a job
queue. There is no broker process and no coordinator. The system has four
participants:

- The **producer** is the user's application code. It enqueues jobs and
  schedules, and is passive after the insert; it may optionally await
  completion.
- **PostgreSQL** is the passive hub: source of truth for all state *and*
  the signal bus (`LISTEN`/`NOTIFY`). It never calls anyone; everyone calls
  it.
- The **worker process** (`pgqueuer/core/`) is the active party. It runs
  the `QueueManager` and `SchedulerManager` loops: listen, claim, execute,
  record. The scaling unit is the process; run more of them for more
  throughput.
- The **operator** is a human. They install and upgrade the schema, start
  workers, and observe via CLI, dashboard, or MCP server.

## Architecture

```
┌────────────┐                              ┌────────────┐
│  Producer  │                              │  Operator  │
│ (app code) │                              │  (human)   │
└─────┬──────┘                              └─────┬──────┘
      │ SQL: enqueue,                             │ CLI / dashboard / MCP:
      │ schedule, cancel                          │ install, upgrade, observe
      ▼                                           ▼
┌─────────────────────────────────────────────────────────┐
│  PostgreSQL — source of truth + signal bus              │
│                                                         │
│  • queue table        • schedules table                 │
│  • log table          • statistics table                │
│  • NOTIFY channel     • triggers                        │
└───────────────────────────┬─────────────────────────────┘
              ▲             │
              │ SQL: claim, │ LISTEN/NOTIFY: wake-up
              │ heartbeat,  │ signals (no job data)
              │ log status  ▼
┌─────────────────────────────────────────────────────────┐
│  Worker process (one event loop; N processes to scale)  │
│  QueueManager + SchedulerManager                        │
└─────────────────────────────────────────────────────────┘
```

Notifications are wake-up signals only; job data always travels over SQL
(ADR-0003). Any worker may claim any eligible job by winning a row lock;
there is no assignment (ADR-0002).

## Use cases

Three nested use cases run top-down; a fourth, `Run schedules` (UC2b), sits
beside `Process jobs` inside the same worker process.

```
Operate installation ──includes──► Process jobs ──includes──► Execute job
```

### UC1: Operate installation (user-goal level)

| Aspect        | Description                                        |
|---------------|----------------------------------------------------|
| Primary actor | Operator (human)                                   |
| Input         | A PostgreSQL database, **Durability** level        |
| Output        | A running, observable **Installation**             |

Steps:

1. Install schema (`pgq install`), choosing durability and namespace.
2. Start one or more worker processes (`pgq run <factory>`).
3. Observe queue depth, throughput, failures, and worker liveness via
   dashboard, MCP, Prometheus, or CLI.
4. Upgrade schema on library upgrades (`pgq upgrade`).

### UC2: Process jobs (subfunction level)

| Aspect        | Description                                        |
|---------------|----------------------------------------------------|
| Primary actor | `QueueManager`                                     |
| Input         | Registered **Entrypoints**, concurrency limits     |
| Output        | Stream of executed **Jobs**, **Log** entries       |

Steps:

1. Wait for a NOTIFY event or poll timeout.
2. Claim a batch of eligible jobs
   ([`FOR UPDATE SKIP LOCKED`](../reference/skip-locked.md)), respecting
   global concurrency limits (ADR-0006) and stale-job re-pick (ADR-0005).
3. Execute each job (UC3).
4. Record outcome to the log; repeat from step 1.

Alternatives: in `drain` mode (`QueueExecutionMode.drain`), exit when the
queue is empty instead of returning to step 1.

### UC3: Execute job (subfunction level)

| Aspect        | Description                                        |
|---------------|----------------------------------------------------|
| Primary actor | Entrypoint executor                                |
| Input         | One claimed **Job**                                |
| Output        | Terminal **Job** status + **Log** entry            |

Steps:

1. Route the job to its registered entrypoint by name.
2. Run the async entrypoint with a `Context` (cancellation scope, shared
   resources); buffer heartbeats while it runs.
3. Classify the outcome: `successful`, `exception`, `canceled`, or,
   depending on retry and `on_failure` policy, re-queue or `failed`.

### UC2b: Run schedules (subfunction level)

| Aspect        | Description                                        |
|---------------|----------------------------------------------------|
| Primary actor | `SchedulerManager`                                 |
| Input         | Registered **Schedules** (cron expression + name)  |
| Output        | Executed scheduled tasks, updated `next_run`       |

Steps:

1. Register schedules as rows in the schedules table (ADR-0022).
2. Poll for due schedules; claim ownership via row lock and heartbeat.
3. Execute the scheduled entrypoint; compute and store the next run time.

## Runtime flow

The [job flow diagram](../reference/architecture.md#job-flow-diagram) in
the architecture reference draws the path from enqueue to completion. At
model level, three facts matter: enqueue can share a transaction with the
producer's business writes (ADR-0001); a poll safety net covers missed or
lost notifications (ADR-0003); and outcomes append to the **Log**, from
which statistics are derived by later aggregation (ADR-0008).

Delivery is at-least-once: a crash between claim and completion re-delivers,
so entrypoints must be idempotent (ADR-0004).

## Model

Arrows denote dependency, not communication flow.

```
┌──────────────┐      ┌───────────┐       ┌──────────┐
│              │─────►│   Queue   │──────►│   Job    │
│              │ 1    └───────────┘ 0..*  └────┬─────┘
│              │                               │
│ Installation │      ┌───────────┐            ├──► Entrypoint name
│ (namespace)  │─────►│ Schedules │            ├──► Payload (opaque bytes)
│              │ 1    └─────┬─────┘            ├──► Priority, execute_after
│              │            │ 0..*             ├──► Attempts (retry state)
│              │            ▼                  ├──► Heartbeat (liveness)
│              │      ┌───────────┐            └──► Headers (tracing)
│              │      │ Schedule  │──► CronExpression
│              │      └───────────┘
│              │
│              │      ┌───────────┐       ┌───────────┐
│              │─────►│    Log    │──────►│ Log entry │──► TracebackRecord
└──────────────┘ 1    └───────────┘ 0..*  └───────────┘
```

### Entities

| Component    | Type   | Description                                          |
|--------------|--------|------------------------------------------------------|
| Installation | Entity | One namespaced set of DB objects; several may share a database (ADR-0017) |
| Job          | Entity | Unit of work; identity `JobId` (`domain/types.py`)   |
| Schedule     | Entity | Recurring cron-driven task; identity `ScheduleId`    |
| Log entry    | Entity | Append-only record of one status transition          |

### Value objects

| Component        | Type         | Description                                    |
|------------------|--------------|------------------------------------------------|
| JobId, ScheduleId| Value Object | `NewType` identities (`domain/types.py`)       |
| Entrypoint       | Value Object | Name binding a job to its registered handler; schedules use the `CronEntrypoint` variant |
| Payload          | Value Object | Opaque `bytes`; the library ships no serializer (ADR-0009) |
| Headers          | Value Object | Side-channel dict for tracing propagation      |
| Job status       | Value Object | `queued / picked / successful / exception / canceled / deleted / failed` |
| CronExpression   | Value Object | When a Schedule fires                          |
| Event            | Value Object | NOTIFY payload: table-changed, cancellation, or health-check; signal only, never job data |
| Durability       | Value Object | Install-time crash-safety level (ADR-0010)     |
| OnConflict       | Value Object | Dedupe policy on enqueue (ADR-0011)            |
| OnFailure        | Value Object | `delete` or `hold` disposition after final failure |
| TracebackRecord  | Value Object | Captured exception detail on a Log entry       |
| Statistics       | Read model   | Aggregates derived from the Log, not maintained inline (ADR-0008) |

### Services

| Component        | Type    | Description                                      |
|------------------|---------|--------------------------------------------------|
| QueueManager     | Service | Claim/dispatch loop, concurrency, health (`core/qm.py`) |
| SchedulerManager | Service | Due-schedule dispatch loop (`core/sm.py`)        |
| EventRouter      | Service | Routes NOTIFY events to waiters (`core/listeners.py`) |
| Executors        | Service | Run user entrypoints; retry variants (`core/executors.py`) |
| Persistence ports| Port    | `QueueRepositoryPort`, `ScheduleRepositoryPort`, `NotificationPort`, `SchemaManagementPort` (`ports/`); satisfied by the `Queries` adapter |

## State machines

### Job status

The canonical diagram lives in the
[architecture reference](../reference/architecture.md#job-status-lifecycle).
Summary: `queued` → `picked` → one of `successful / exception / canceled /
failed`; `deleted` for removal without running; retry re-queues with
persisted attempt state (ADR-0007).

### QueueManager loop

The canonical diagram lives in the
[architecture reference](../reference/architecture.md#queuemanager-processing-loop).
Summary: wait → claim → dispatch → record, back to wait; `drain` mode exits
when the queue is empty instead.

Invariant: there is no coordinator. The claim query decides eligibility;
locks, concurrency gates, and stale-heartbeat re-pick all live in the claim
SQL (ADR-0002, ADR-0005, ADR-0006).

### Schedule lifecycle

A schedule cycles due → claimed → executed → rescheduled (`next_run`
recomputed). Runners compete for due schedules the same way workers compete
for jobs: row-lock claim plus heartbeat-based staleness recovery (ADR-0022).

## Sub-models

- [Dequeue composition model](dequeue-composition.md): how the claim
  statement is assembled from the concurrency gates in use, with the
  shapes, bind order, invariants, and guarding tests (ADR-0024).

Planned split-outs once a section outgrows this document; each keeps a
summary here:

- Job lifecycle model: statuses, retries, cancellation, and completion
  tracking in one place.
- Scheduling model: schedule ownership, cadence, and cron semantics.
- Schema & namespace model: installation objects, durability policies, and
  the migration stream.
- Observability model: the log/statistics pipeline, dashboard, metrics,
  and the MCP read surface.
