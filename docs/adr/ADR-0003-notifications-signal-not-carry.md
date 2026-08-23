# ADR-0003: Notifications signal that something changed, never carry job data

## Status

Accepted (retroactive: documents existing behavior)

## Context

With workers claiming jobs by lock contention (ADR-0002), something has
to tell an idle worker that new work exists. PostgreSQL's LISTEN/NOTIFY
can deliver that signal, and putting the job itself in the payload looks
attractive because the worker could start immediately. NOTIFY makes a
poor delivery channel, though: payloads are capped at about 8 kB,
notifications sent in a rolled-back transaction vanish, and a worker
that connects a moment too late misses everything sent before it
listened.

## Decision

LISTEN/NOTIFY wakes workers up; it never delivers work. A notification
payload holds a small routing envelope, such as the event type and, for
cancellations, the affected job ids. It never holds a job payload. On a
wake-up the worker always goes back to the queue table and claims work
through the ordinary query (ADR-0002). A periodic poll runs regardless,
so a worker that never hears a notification still finds the work.

## Consequences

### Positive consequences

- Lost, duplicated, or reordered notifications are harmless: the queue
  table is the source of truth, and the poll safety net covers gaps.
- The 8 kB NOTIFY payload limit is irrelevant to job size.
- Latency stays low when a notification does arrive, and nothing breaks
  when one does not.

### Negative consequences

- Every wake-up costs an extra round-trip: the notification arrives,
  then the worker queries for the actual work.
- Worst-case pickup latency for a missed notification is the poll
  interval, not zero.
- A burst of enqueues produces a burst of notifications that all trigger
  the same query; debouncing is needed to avoid redundant round-trips.

## Alternatives considered

### Push job payloads through NOTIFY

The worker could start without a second round-trip. Rejected: payloads
over 8 kB cannot be sent at all, a missed notification means a lost job
unless a poll backstop exists anyway, and two delivery paths (payload
push plus poll) must then agree with each other.

### No notifications, poll only

This would be simpler, with one mechanism instead of two. Rejected:
pickup latency becomes the poll interval for every job, and shortening
the interval trades latency for constant idle query load.

## Not covered by this ADR

Channel naming (the backlogged namespacing record applies), poll and debounce
intervals, the JSON shape of the event envelope, completion watching
built on the same signal (consequence of this record).

## References

- [ADR index and backlog](README.md)
- Event routing: `pgqueuer/core/listeners.py`
