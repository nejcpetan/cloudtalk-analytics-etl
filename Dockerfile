FROM python:3.13-slim-bookworm

# Security: create non-root user
RUN groupadd --system etl && useradd --system --gid etl etl

# Install cron (supercronic for Docker-friendly cron)
ARG SUPERCRONIC_VERSION=v0.2.33
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
        ca-certificates \
    && curl -fsSL -o /usr/local/bin/supercronic \
        "https://github.com/aptible/supercronic/releases/download/${SUPERCRONIC_VERSION}/supercronic-linux-amd64" \
    && chmod +x /usr/local/bin/supercronic \
    && apt-get purge -y --auto-remove curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy everything needed to build and install the package
COPY pyproject.toml ./
COPY src/ ./src/
# Default installs PostgreSQL driver only.
# For MySQL: build with --build-arg INSTALL_TARGET='.[mysql]'
ARG INSTALL_TARGET="."
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir "$INSTALL_TARGET"

COPY scripts/ ./scripts/

# Create crontab entrypoint
COPY scripts/entrypoint.sh /entrypoint.sh
# Strip Windows CRLF line endings (if committed from Windows) and make executable
RUN sed -i 's/\r$//' /entrypoint.sh && chmod +x /entrypoint.sh

# Bake build timestamp into the image — shows in logs so you can verify deployment date.
RUN date -u +"%Y-%m-%d %H:%M UTC" > /app/built_at.txt

# Create log directory
RUN mkdir -p /app/logs && chown etl:etl /app/logs

USER etl

ENTRYPOINT ["/entrypoint.sh"]
