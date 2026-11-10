# syntax=docker/dockerfile:1
FROM python:3.12.12-slim-bookworm AS runtime

COPY --from=ghcr.io/astral-sh/uv:0.12.19 /uv /bin/uv

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    PATH=/opt/venv/bin:$PATH

WORKDIR /app
RUN useradd --create-home --uid 10001 app

# Dependencies first so code changes don't invalidate the dependency layer.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY --chown=app:app . .
USER app

EXPOSE 8000
# Migrations run on boot; they are idempotent (alembic tracks the applied revision).
CMD ["sh", "-c", "alembic upgrade head && exec uvicorn app.main:app --host 0.0.0.0 --port 8000"]


# Same image plus lint/type/security/test tooling; used by `make test`.
FROM runtime AS dev
USER root
RUN uv sync --frozen --no-install-project
USER app
# The app directory stays read-only for the app user; tool caches go to /tmp.
ENV RUFF_CACHE_DIR=/tmp/ruff-cache \
    MYPY_CACHE_DIR=/tmp/mypy-cache \
    COVERAGE_FILE=/tmp/.coverage \
    PYTEST_ADDOPTS="-p no:cacheprovider"
CMD ["./scripts/check.sh"]
