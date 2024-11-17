ARG POSTGRES_VERSION=16
FROM postgres:${POSTGRES_VERSION}
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ENV POSTGRES_USER=testuser
ENV POSTGRES_PASSWORD=testpassword

# asyncpg environment variables
ENV PGHOST=/var/run/postgresql
ENV PGUSER=testuser
ENV PGPASSWORD=testpassword
ENV PGPORT=5432

# Install dependencies and uv
RUN apt-get update && \
    apt-get install -y git dos2unix && \
    rm -rf /var/lib/apt/lists/*

# Copy application source code
COPY . /src/
WORKDIR /src

# Install project dependencies using uv
RUN uv sync --frozen --group 'asyncpg'

# Copy and prepare the database initialization script
COPY --chmod=0755 test/db/init_db.sh /docker-entrypoint-initdb.d/init_db.sh
RUN dos2unix /docker-entrypoint-initdb.d/init_db.sh