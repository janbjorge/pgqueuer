# PgQueuer agent guide

Use this guide to help a human understand, set up, or troubleshoot PgQueuer.
It is a route map, not a second copy of the documentation. Canonical facts,
commands, examples, and configuration live at
<https://janbjorge.github.io/pgqueuer/>.

## How to guide the human

1. Ask what they are building, whether PostgreSQL is already available, and
   whether their application uses asyncpg or psycopg.
2. Read [When to Use PgQueuer] before explaining the trade-offs. Help them
   decide whether PgQueuer fits; do not assume that it does.
3. Read [Core Concepts] and explain jobs, entrypoints, producers, consumers,
   job states, schedules, and drivers in that order.
4. Read [Installation] and walk through only the path matching their package
   manager, driver, and database setup. Pause before commands that modify their
   database.
5. Read [Quick Start] and help them process one job end to end. Adapt its
   example to their application instead of introducing additional features.
6. Confirm that the first job completes before offering reliability,
   scheduling, observability, or deployment features.

[When to Use PgQueuer]: https://janbjorge.github.io/pgqueuer/getting-started/when-to-use/
[Core Concepts]: https://janbjorge.github.io/pgqueuer/getting-started/core-concepts/
[Installation]: https://janbjorge.github.io/pgqueuer/getting-started/installation/
[Quick Start]: https://janbjorge.github.io/pgqueuer/getting-started/quickstart/

## Route questions to canonical pages

- Driver selection and connection patterns:
  <https://janbjorge.github.io/pgqueuer/reference/drivers/>
- CLI commands and schema management:
  <https://janbjorge.github.io/pgqueuer/reference/cli/>
- Retries and failure handling:
  <https://janbjorge.github.io/pgqueuer/guides/reliability/>
- Scheduling:
  <https://janbjorge.github.io/pgqueuer/guides/scheduling/>
- Concurrency:
  <https://janbjorge.github.io/pgqueuer/guides/concurrency-control/>
- Testing without PostgreSQL:
  <https://janbjorge.github.io/pgqueuer/reference/in-memory/>
- Deployment and performance:
  <https://janbjorge.github.io/pgqueuer/guides/deployment/> and
  <https://janbjorge.github.io/pgqueuer/guides/performance-tuning/>
- Monitoring a live queue from an agent:
  <https://janbjorge.github.io/pgqueuer/integrations/mcp-server/>

## Troubleshooting route

Start with
<https://janbjorge.github.io/pgqueuer/development/troubleshooting/>.
For schema errors, also read the [CLI reference]. For delayed or sleeping
workers, also read [Performance Tuning]. For held or retried jobs, read
[Reliability].

[CLI reference]: https://janbjorge.github.io/pgqueuer/reference/cli/
[Performance Tuning]: https://janbjorge.github.io/pgqueuer/guides/performance-tuning/
[Reliability]: https://janbjorge.github.io/pgqueuer/guides/reliability/

Ask for the exact exception, command output, driver, and transaction context
before diagnosing. Prefer a documented check over a speculative fix.

## Rules for you

- Fetch the relevant linked page before giving a command, flag, environment
  variable, API signature, or code example. Do not rely on this route map for
  product details.
- Walk through setup one verified step at a time. Explain what a command will
  change before asking the human to run it.
- Do not edit application files, database objects, or agent configuration
  without the human's approval.
- Do not substitute advice for Celery, RQ, Dramatiq, Redis, or another queue.
- `AGENTS.md` in the PgQueuer repository is contributor guidance for changing
  PgQueuer itself. It is not the user setup guide.
