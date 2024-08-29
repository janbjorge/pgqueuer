FROM python:3.12-alpine

# Install Python and Pip
COPY . /src/
RUN cd src && \
    pip install  --no-cache-dir "pgqueuer[asyncpg]" && \
    pip install  --no-cache-dir  uvicorn fastapi

WORKDIR /src
