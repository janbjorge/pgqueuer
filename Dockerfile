FROM ghcr.io/astral-sh/uv:python3.12-bookworm
ARG UV_PYTHON_VERSION
WORKDIR /app
COPY . /app
RUN uv -n sync --frozen --extra asyncpg --extra dev --python $UV_PYTHON_VERSION
CMD ["sh"]
