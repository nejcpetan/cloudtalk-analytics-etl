FROM python:3.13-slim-bookworm

# Security: create non-root user
RUN groupadd --system etl && useradd --system --gid etl etl

# Install cron (supercronic for Docker-friendly cron)
ARG SUPERCRONIC_VERSION=v0.2.33
ARG SUPERCRONIC_SHA256=71b0d58cc53f6bd72f4f2c7e935f348f9b2a4c8405de4e1a3b9e11aa93b1f7da
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
        ca-certificates \
    && curl -fsSLO "https://github.com/aptible/supercronic/releases/download/${SUPERCRONIC_VERSION}/supercronic-linux-amd64" \
    && echo "${SUPERCRONIC_SHA256}  supercronic-linux-amd64" | sha256sum -c - \
    && chmod +x supercronic-linux-amd64 \
    && mv supercronic-linux-amd64 /usr/local/bin/supercronic \
    && apt-get purge -y --auto-remove curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install dependencies first (layer caching)
COPY pyproject.toml ./
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir .

# Copy source code
COPY src/ ./src/
COPY scripts/ ./scripts/

# Create crontab entrypoint
COPY scripts/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Create log directory
RUN mkdir -p /app/logs && chown etl:etl /app/logs

USER etl

ENTRYPOINT ["/entrypoint.sh"]
