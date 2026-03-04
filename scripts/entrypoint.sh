#!/bin/bash
set -e

CRON_SCHEDULE="${CRON_SCHEDULE:-0 2 * * *}"

echo "CloudTalk ETL Service starting..."
echo "Schedule: ${CRON_SCHEDULE}"
echo "Log level: ${LOG_LEVEL:-INFO}"

# Generate crontab from environment variable
printf '%s /usr/local/bin/python -m cloudtalk_etl\n' "${CRON_SCHEDULE}" > /tmp/crontab

# If first argument is "run", execute ETL immediately (for testing / manual trigger)
if [ "$1" = "run" ]; then
    echo "Running ETL immediately..."
    exec /usr/local/bin/python -m cloudtalk_etl
fi

# Otherwise, start supercronic (foreground, logs to stdout)
echo "Starting cron daemon..."
exec supercronic /tmp/crontab
