# Multi-stage: the build stage carries uv and the lockfile, the runtime stage does not.
FROM ghcr.io/astral-sh/uv:python3.14-bookworm-slim AS builder

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never

WORKDIR /app

# Dependencies resolve from the lockfile only, in their own layer, so editing source
# code does not invalidate the (slow) dependency install.
# README.md is referenced by pyproject's `readme` field, so the build backend needs it.
COPY pyproject.toml uv.lock README.md ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-dev --no-editable

COPY src/ ./src/
# --no-editable installs the package into site-packages, so the runtime stage needs
# only the venv, not the source tree.
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable


FROM python:3.14-slim-bookworm AS runtime

# Never run as root: a container escape should not land on a root shell.
RUN useradd --create-home --uid 10001 ingest

WORKDIR /app
COPY --from=builder --chown=ingest:ingest /app/.venv /app/.venv

# The poll-target list is reference data, baked in deliberately: it is small, and the
# container must not need the DB APIs just to learn which stations to poll.
# Regenerate it with scripts/pick_poll_targets.py when the scope changes, or mount a
# volume and override POLL_TARGETS_PATH.
COPY --chown=ingest:ingest data/raw/stada/poll_targets.json /app/data/raw/stada/poll_targets.json

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    POLL_TARGETS_PATH=/app/data/raw/stada/poll_targets.json \
    STATE_DIR=/app/data/state \
    OUTPUT_DIR=/app/data/out \
    LOG_FORMAT=json

RUN mkdir -p /app/data/out /app/data/state && chown -R ingest:ingest /app/data
USER ingest

# Exec form, no shell wrapper: PID 1 is python itself, so SIGTERM from `docker stop`
# reaches the signal handler instead of being swallowed by /bin/sh.
#
# ENTRYPOINT is just `python` so the same image can run either process: the ingestion
# service (default CMD) or the bridge (compose overrides `command`). They share a
# package and dependencies, so a second image would be pure duplication.
ENTRYPOINT ["python"]
CMD ["-m", "railway_network_analytics"]
