FROM ghcr.io/astral-sh/uv:python3.12-bookworm
WORKDIR /app
COPY . /app
RUN uv -n sync --frozen
CMD ["sh"]
