# Callable factory example

This example demonstrates passing a callable factory to `pgqueuer.cli.run`.

## Setup

1. Create the test database:

   ```bash
   docker compose up populate
   ```

2. Install dependencies from the project root:

   ```bash
   uv sync
   uv pip install "psycopg[binary]"
   uv pip install .
   ```

3. Export environment variables:

   ```bash
   export POSTGRES_USER=pgquser
   export POSTGRES_DB=pgqdb
   export POSTGRES_PASSWORD=pgqpw
   export POSTGRES_HOST=localhost
   export POSTGRES_PORT=5432
   ```

4. Start the consumer and the producer:

   ```bash
   uv run python consumer.py --components heart_beat,head_stand
   uv run python producer.py
   ```
