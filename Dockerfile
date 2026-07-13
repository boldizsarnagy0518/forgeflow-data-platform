FROM python:3.12-slim AS builder

ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
COPY --from=ghcr.io/astral-sh/uv:0.11.16 /uv /uvx /bin/
WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
RUN uv sync --locked --no-dev

FROM python:3.12-slim AS runtime
ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1
RUN groupadd --system forgeflow && useradd --system --gid forgeflow --home /app forgeflow
WORKDIR /app
COPY --from=builder --chown=forgeflow:forgeflow /app /app
COPY --chown=forgeflow:forgeflow dbt ./dbt
COPY --chown=forgeflow:forgeflow infra ./infra
USER forgeflow
EXPOSE 8000 8501
CMD ["forgeflow-api"]
