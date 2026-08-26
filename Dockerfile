# pMax Performance Pack, packaged as a Cloud Run Job.
# python:3.12-slim
FROM python:3.12-slim@sha256:7a8b475003c4fe15a2cd4e55e5cfc2f3560bdc9333d624f24cdd6d4340fd7a17

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/app/.venv/bin:$PATH" \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

# uv 0.10.4
COPY --from=ghcr.io/astral-sh/uv:0.10.4@sha256:4cac394b6b72846f8a85a7a0e577c6d61d4e17fe2ccee65d9451a8b3c9efb4ac /uv /uvx /bin/

RUN useradd --system --uid 10001 --create-home --user-group app \
    && mkdir -p /app \
    && chown app:app /app

WORKDIR /app

COPY --chown=app:app pyproject.toml uv.lock README.md LICENSE NOTICE ./
COPY --chown=app:app src/ src/

USER app

RUN uv sync --locked --no-dev --no-editable

ENTRYPOINT ["pmax-pack"]
