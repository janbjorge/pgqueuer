FROM python:3.12-alpine

# Install Python and Pip
COPY tools/ /src/
RUN cd src && \
    pip install  --no-cache-dir "pgqueuer[asyncpg]" && \
    pip install  --no-cache-dir uvicorn fastapi

WORKDIR /src
CMD ["uvicorn", "--factory", "prometheus.prometheus:create_app", "--host", "0.0.0.0", "--port", "8000"]
