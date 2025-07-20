FROM ghcr.io/astral-sh/uv:python3.12-bookworm
WORKDIR /app
COPY . /app
RUN uv pip install .
ENTRYPOINT ["pgq", "run"]
