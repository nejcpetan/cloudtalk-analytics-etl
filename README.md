# CloudTalk Analytics ETL Service

Nightly ETL service that extracts call, agent, and group statistics data from the
**CloudTalk REST API** and loads it into a **PostgreSQL** (Neon or MySQL) database for
consumption by Qlik Sense BI dashboards.

Runs on a cron schedule (default: 02:00 Europe/Ljubljana) inside Docker via `supercronic`.
Deployed and managed through Portainer.

---

## Table of Contents

1. [How It Works](#how-it-works)
2. [Technology Stack](#technology-stack)
3. [Project Structure](#project-structure)
4. [Database Schema](#database-schema)
5. [Environment Variables](#environment-variables)
6. [Database Backend — PostgreSQL vs MySQL](#database-backend--postgresql-vs-mysql)
7. [Local Development Setup](#local-development-setup)
8. [Running Tests](#running-tests)
9. [Docker / Deployment](#docker--deployment)
10. [Operations — Day to Day](#operations--day-to-day)
11. [Backfilling Historical Data](#backfilling-historical-data)
12. [Troubleshooting](#troubleshooting)

---

## How It Works

### Big picture

Every night at 02:00 the container wakes up and runs a Python ETL pipeline that:

1. **Extracts** the previous day's call index from the CloudTalk API
2. **Fetches details** for each call from the analytics API — this is the authoritative source for group names and per-agent step data (throttled at 1050ms per request to stay within the 60 req/min limit)
3. **Transforms** the raw JSON into clean, flat rows aggregated across 3 output tables
4. **Loads** the rows into PostgreSQL (Neon or MySQL) using upserts

The job syncs **yesterday** by default (e.g. running at 02:00 on March 5th syncs all of March 4th).
This guarantees a complete day is always captured before the job runs.

### What data is captured

| Table | Source | Granularity |
|-------|--------|-------------|
| `call_center_groups` | `/calls/{callId}` detail, `QueueStep.name` | One row per (date, group) |
| `agent_stats` | `/calls/{callId}` detail, `QueueStep.agent_calls` | One row per (date, group, agent) |
| `call_reasons` | `/calls/{callId}` detail, `call_tags[].label` | One row per (date, group, tag) |

### Group assignment

The authoritative group name for each call comes from the first `QueueStep.name` in the
call's `call_steps` array (fetched from the detail endpoint). If a call has no queue step,
the `internal_number.name` is used as a fallback. Calls where neither is available are
silently skipped (not counted in any group).

Group names follow the format `"Category - SLO"` or `"Category - CRO"`. The ETL parses
these into `country` (`SLO`/`CRO`) and `category` columns automatically. Phone line groups
in parenthesis format `(SLO) Name` are filtered out as junk.

### Rate limiting and retries

CloudTalk's API enforces a 60 req/min limit. The ETL uses a **token bucket rate limiter**
set to 50 req/min (a safety margin). Additionally, detail fetches have a hard 1050ms minimum
delay between requests. All API calls are wrapped with **tenacity** retries: up to 5 attempts
with exponential backoff + jitter, handling transient 5xx errors and 429 responses.

### Upserts, not deletes

All writes use `INSERT ... ON CONFLICT DO UPDATE` (PostgreSQL) or `ON DUPLICATE KEY UPDATE`
(MySQL). Re-running the ETL for the same date is always safe — no duplicate rows and no
historical data is destroyed.

---

## Technology Stack

| Component | Version | Purpose |
|-----------|---------|---------|
| Python | 3.13 | Runtime |
| httpx | 0.28.x | Sync HTTP client for CloudTalk API |
| pydantic-settings | 2.7.x | Typed config from env vars |
| structlog | 25.1.x | Structured JSON logging to stdout |
| psycopg (v3) | 3.3.x | PostgreSQL driver (default; Neon requires SSL) |
| mysql-connector-python | 9.x | MySQL driver (optional; install with `.[mysql]`) |
| tenacity | 9.1.x | Retry logic with exponential backoff |
| supercronic | 0.2.33 | Cron scheduler inside Docker |
| Docker base image | `python:3.13-slim-bookworm` | Minimal production image |

---

## Project Structure

```
cloudtalk-etl/
├── pyproject.toml              # Project metadata + dependencies
├── Dockerfile                  # Production container image
├── docker-compose.yml          # Compose file (dev + Portainer)
├── .env.example                # Environment variable template — copy to .env
├── .gitattributes              # Forces LF line endings on .sh files (Windows safety)
│
├── .schema/                    # Table DDL for the DWH team
│   ├── call_center_groups.sql
│   ├── agent_stats.sql
│   └── call_reasons.sql
│
├── src/cloudtalk_etl/
│   ├── __main__.py             # Entry point: python -m cloudtalk_etl
│   ├── config.py               # All env var config (pydantic-settings)
│   ├── logging.py              # Structured JSON logging setup
│   ├── main.py                 # ETL orchestrator — calls extract/transform/load
│   │
│   ├── api/
│   │   ├── client.py           # CloudTalk HTTP client (auth, retry, pagination)
│   │   │                       # Handles two base URLs: index (my.cloudtalk.io)
│   │   │                       # and detail (analytics-api.cloudtalk.io)
│   │   └── rate_limiter.py     # Token bucket rate limiter (50 req/min)
│   │
│   ├── db/
│   │   ├── backend.py          # Backend selector — reads DB_BACKEND env var
│   │   ├── connection.py       # PostgreSQL connection (psycopg3, SSL)
│   │   ├── connection_mysql.py # MySQL connection (mysql-connector-python)
│   │   ├── schema.py           # PostgreSQL DDL (CREATE TABLE IF NOT EXISTS)
│   │   ├── schema_mysql.py     # MySQL DDL
│   │   ├── repositories.py     # PostgreSQL upsert functions (ON CONFLICT)
│   │   └── repositories_mysql.py # MySQL upsert functions (ON DUPLICATE KEY)
│   │
│   └── etl/
│       ├── extract.py          # Pull call index + details from CloudTalk API
│       ├── transform.py        # Aggregate raw detail JSON into 3 table shapes
│       └── load.py             # Write to DB via backend repositories
│
├── scripts/
│   └── entrypoint.sh           # Docker entrypoint: starts supercronic or runs once
│
└── tests/
    ├── conftest.py             # Shared fixtures (sample call detail response)
    ├── test_api_client.py      # HTTP client + pagination (network mocked)
    ├── test_rate_limiter.py    # Token bucket behaviour
    ├── test_repositories.py    # Upsert SQL (DB mocked)
    ├── test_transform.py       # Transform logic, group name parsing, date formatting
    └── test_etl.py             # Extract pipeline (API mocked)
```

---

## Database Schema

Tables are created automatically when the ETL container first starts — you do not need
to run any SQL manually. The 3 table DDL files are also available in `.schema/` for the
DWH team to replicate the structure in an internal database.

### `call_center_groups`

One row per (date, country, group). Primary key: `(date, country, group_name)`.

| Column | Type | Description |
|--------|------|-------------|
| `date` | TEXT | Date in `DD.MM.YYYY` format, e.g. `"09.03.2026"` |
| `country` | TEXT | `"SLO"` or `"CRO"` (parsed from group name suffix) |
| `group_name` | TEXT | Full queue name, e.g. `"Reklamacije - SLO"` |
| `category` | TEXT | Parsed category, e.g. `"Reklamacije"` |
| `total_calls` | INTEGER | Total calls routed to this group |
| `answered_calls` | INTEGER | Calls answered by an agent |
| `answered_pct` | NUMERIC(5,2) | `answered / total * 100`, NULL if total = 0 |
| `unanswered_calls` | INTEGER | Calls not answered (missed/abandoned) |
| `synced_at` | TIMESTAMPTZ | When this row was last written |

### `agent_stats`

One row per (date, country, group, agent). Primary key: `(date, country, group_name, agent_id)`.
An agent appears in multiple rows if they handle calls in multiple queues.

| Column | Type | Description |
|--------|------|-------------|
| `date` | TEXT | Date in `DD.MM.YYYY` format |
| `country` | TEXT | `"SLO"` or `"CRO"` |
| `group_name` | TEXT | Full queue name |
| `category` | TEXT | Parsed category |
| `agent_id` | INTEGER | CloudTalk internal agent ID |
| `agent_name` | TEXT | Agent full name (NULL if unavailable) |
| `presented_calls` | INTEGER | Times the agent's phone rang (every ring = 1) |
| `answered_calls` | INTEGER | Calls the agent actually picked up |
| `talking_time_sec` | INTEGER | Total seconds spent talking (answered calls only) |
| `synced_at` | TIMESTAMPTZ | When this row was last written |

### `call_reasons`

One row per (date, country, group, tag). Primary key: `(date, country, group_name, tag_id)`.
Only calls that have at least one tag are counted.

| Column | Type | Description |
|--------|------|-------------|
| `date` | TEXT | Date in `DD.MM.YYYY` format |
| `country` | TEXT | `"SLO"` or `"CRO"` |
| `group_name` | TEXT | Full queue name |
| `category` | TEXT | Parsed category |
| `tag_id` | INTEGER | CloudTalk internal tag ID |
| `tag_name` | TEXT | Tag label text (NULL if unavailable) |
| `call_count` | INTEGER | Number of calls with this tag in this group on this date |
| `synced_at` | TIMESTAMPTZ | When this row was last written |

---

## Environment Variables

Copy `.env.example` to `.env` and fill in the required values.

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `CLOUDTALK_API_KEY_ID` | ✅ | — | CloudTalk API key ID. Generate in CloudTalk → Settings → API |
| `CLOUDTALK_API_KEY_SECRET` | ✅ | — | CloudTalk API key secret |
| `DATABASE_URL` | ✅ | — | PostgreSQL: full Neon connection string incl. `?sslmode=require`. MySQL: `mysql://user:pass@host:3306/dbname` |
| `DB_BACKEND` | — | `postgresql` | `"postgresql"` or `"mysql"` — selects the database driver |
| `CRON_SCHEDULE` | — | `0 2 * * *` | When the ETL runs (cron syntax). Default = 02:00 every night |
| `LOG_LEVEL` | — | `INFO` | Log verbosity: `DEBUG`, `INFO`, or `WARNING` |
| `RATE_LIMIT_RPM` | — | `50` | API requests per minute. CloudTalk's limit is 60 — keep this ≤ 50 |
| `ETL_DATE_OVERRIDE` | — | yesterday | Force a specific sync date in `YYYY-MM-DD` format. Used for backfills |
| `TEST_MODE` | — | `false` | If `true`, fetches the full call index but samples only `TEST_SAMPLE_SIZE` call details |
| `TEST_SAMPLE_SIZE` | — | `50` | Number of call details to fetch when `TEST_MODE=true` |
| `TZ` | — | — | Timezone for the cron schedule. Set to `Europe/Ljubljana` in the compose file |

---

## Database Backend — PostgreSQL vs MySQL

The ETL supports both PostgreSQL (default) and MySQL 8.0+. The backend is selected at
runtime via the `DB_BACKEND` environment variable — no code changes needed.

### Side-by-side comparison

| Setting | PostgreSQL (Neon) | MySQL |
|---------|-------------------|-------|
| `DB_BACKEND` | `postgresql` | `mysql` |
| `DATABASE_URL` | `postgresql://user:pass@ep-xxx.neon.tech/neondb?sslmode=require` | `mysql://user:pass@db.host.com:3306/cloudtalk` |
| Docker build | `docker build -t cloudtalk-etl .` | `docker build --build-arg INSTALL_TARGET='.[mysql]' -t cloudtalk-etl .` |
| compose `build:` | `build: .` | uncomment the `build: context/args` block in `docker-compose.yml` |

### Using PostgreSQL (default — Neon)

Set these in your `.env` or Portainer stack environment:

```env
DB_BACKEND=postgresql
DATABASE_URL=postgresql://neondb_owner:YOUR_PASSWORD@ep-xxx.eu-central-1.aws.neon.tech/neondb?sslmode=require
```

Build normally:
```bash
docker build -t cloudtalk-etl .
```

### Using MySQL

Set these in your `.env` or Portainer stack environment:

```env
DB_BACKEND=mysql
DATABASE_URL=mysql://cloudtalk_user:YOUR_PASSWORD@your-db-host.com:3306/cloudtalk_analytics
```

The MySQL driver (`mysql-connector-python`) is an optional dependency — not included in the
default image. Build with the `mysql` extra:

```bash
docker build --build-arg INSTALL_TARGET='.[mysql]' -t cloudtalk-etl .
```

In `docker-compose.yml`, replace `build: .` with the commented MySQL build block:

```yaml
build:
  context: .
  args:
    INSTALL_TARGET: ".[mysql]"
```

To disable SSL for a local MySQL instance (dev only), append `?ssl_disabled=true`:
```env
DATABASE_URL=mysql://root:secret@localhost:3306/cloudtalk?ssl_disabled=true
```

---

## Local Development Setup

### Prerequisites

- Python 3.13
- Access to the database (Neon connection string, or a local MySQL instance)
- CloudTalk API credentials (Settings → API in the CloudTalk admin)

### 1. Clone the repository

```bash
git clone https://github.com/nejcpetan/cloudtalk-analytics-etl.git
cd cloudtalk-analytics-etl
```

### 2. Create a virtual environment and install dependencies

```powershell
# Windows (PowerShell)
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -e ".[dev]"

# With MySQL support:
pip install -e ".[dev,mysql]"
```

```bash
# Linux / macOS
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

### 3. Configure environment variables

```bash
cp .env.example .env
# Edit .env and fill in:
#   CLOUDTALK_API_KEY_ID
#   CLOUDTALK_API_KEY_SECRET
#   DATABASE_URL
#   DB_BACKEND   (optional, defaults to postgresql)
```

### 4. Run the ETL

```bash
# Full run — syncs yesterday's data
python -m cloudtalk_etl

# Test run — fetches full call index but only 50 call details
TEST_MODE=true python -m cloudtalk_etl

# Sync a specific date
ETL_DATE_OVERRIDE=2026-03-03 python -m cloudtalk_etl
```

---

## Running Tests

Tests are fully offline — no network calls, no database. All HTTP and DB interactions
are mocked.

```bash
python -m pytest tests/ -v
```

Expected output: **92 tests, all passing**.

```bash
# Run a specific test file
python -m pytest tests/test_transform.py -v
```

---

## Docker / Deployment

### How the container works

1. The container starts and runs `entrypoint.sh`
2. `entrypoint.sh` logs the build timestamp (baked in at image build time) so you can verify which version is running in Portainer
3. The entrypoint writes a crontab to `/tmp/crontab` using the `CRON_SCHEDULE` env var
4. `supercronic` reads the crontab and runs `python -m cloudtalk_etl` on schedule
5. Each run logs structured JSON to stdout (visible in Portainer logs)
6. Docker's `init: true` (tini) is set — this is required so supercronic does not run as PID 1

### Building the image

```bash
# PostgreSQL (default)
docker build -t cloudtalk-etl .

# MySQL
docker build --build-arg INSTALL_TARGET='.[mysql]' -t cloudtalk-etl .
```

### Running once manually (from WSL/Linux)

```bash
# Sync yesterday's data
docker run --rm --env-file .env cloudtalk-etl run

# Sync a specific date
docker run --rm --env-file .env -e ETL_DATE_OVERRIDE=2026-03-03 cloudtalk-etl run

# Test mode
docker run --rm --env-file .env -e TEST_MODE=true cloudtalk-etl run
```

### Deploying to Portainer (first time)

1. Push code to GitHub (`git push`)
2. Open Portainer → **Stacks** → **Add stack**
3. Select **Repository** as the build method
4. Set:
   - Repository URL: `https://github.com/nejcpetan/cloudtalk-analytics-etl`
   - Compose path: `docker-compose.yml`
5. Add all required environment variables in the Portainer stack environment settings
6. Click **Deploy the stack**

### Redeploying after a code change

1. `git push` the changes
2. In Portainer → **Stacks** → `cloudtalk-etl` → **Pull and redeploy**

If you changed dependencies or the Dockerfile, you must also delete the old image in
Portainer (Images → delete `cloudtalk-etl`) before redeploying, otherwise the old
cached image will be used.

---

## Operations — Day to Day

### Checking logs in Portainer

Portainer → **Containers** → `cloudtalk-etl` → **Logs**

The container logs the build timestamp at startup so you can confirm the right version
is deployed:

```
CloudTalk ETL Service starting...
Built: 2026-03-10 08:34 UTC
Schedule: 0 2 * * *
Log level: INFO
```

Each ETL run produces structured JSON log lines. A successful run looks like:

```json
{"event": "etl_started",                    "sync_date": "2026-03-09", "test_mode": false}
{"event": "database_connected",              "server": "ep-xxx.neon.tech"}
{"event": "schema_ensured"}
{"event": "calls_extracted",                "count": 724}
{"event": "extracting_call_details",        "total": 724}
{"event": "call_details_extracted",         "fetched": 720, "skipped": 4}
{"event": "upserted_call_center_groups",    "count": 6}
{"event": "upserted_agent_stats",           "count": 48}
{"event": "upserted_call_reasons",          "count": 23}
{"event": "etl_completed",                  "sync_date": "2026-03-09", "call_center_groups_synced": 6, "agent_stats_synced": 48, "call_reasons_synced": 23, "duration_seconds": 847.2}
```

> Note: fetching ~700 call details at 1050ms each takes ~12 minutes. This is expected.

### Triggering a manual run

From Portainer → **Containers** → `cloudtalk-etl` → **Exec**, or via Docker:

```bash
docker exec cloudtalk-etl python -m cloudtalk_etl
```

### Checking what's in the database

Connect to Neon via the Neon console SQL editor or any PostgreSQL client.

```sql
-- Which groups ran today?
SELECT date, country, group_name, total_calls, answered_calls, answered_pct
FROM call_center_groups
WHERE date = '09.03.2026'
ORDER BY country, group_name;

-- Top agents by answered calls for a date
SELECT agent_name, group_name, answered_calls, talking_time_sec
FROM agent_stats
WHERE date = '09.03.2026'
ORDER BY answered_calls DESC;

-- Call reasons breakdown
SELECT group_name, tag_name, call_count
FROM call_reasons
WHERE date = '09.03.2026'
ORDER BY group_name, call_count DESC;

-- Latest sync check
SELECT MAX(synced_at) FROM call_center_groups;
```

---

## Backfilling Historical Data

Use `ETL_DATE_OVERRIDE` in a loop to re-sync a date range:

```bash
for date in 2026-03-01 2026-03-02 2026-03-03; do
  docker run --rm \
    --env-file .env \
    -e TZ=Europe/Ljubljana \
    -e ETL_DATE_OVERRIDE=$date \
    cloudtalk-etl run
done
```

Each run is idempotent — running it twice for the same date is safe.

> Be aware: each day takes ~12 minutes due to the per-call detail throttle.
> A 7-day backfill takes approximately 90 minutes.

---

## Troubleshooting

### Container exits immediately / supercronic crash

**Symptom:** Container exits right after starting.

**Cause:** `supercronic` crashes when run as PID 1 in a slim container.

**Fix:** The compose file has `init: true` which injects Docker's `tini` as PID 1.
Make sure this is present in the compose file.

### CRLF line ending errors in entrypoint.sh

**Symptom:** Container fails with `/entrypoint.sh: not found`.

**Cause:** Git on Windows may have committed the shell script with CRLF line endings.

**Fix:** The Dockerfile strips CRLF at build time (`sed -i 's/\r$//' /entrypoint.sh`).
The `.gitattributes` file forces LF for `.sh` files going forward.

### ETL fails with authentication error (401)

**Cause:** `CLOUDTALK_API_KEY_ID` or `CLOUDTALK_API_KEY_SECRET` is wrong or expired.

**Fix:** Regenerate the API key in CloudTalk → Settings → API and update the env var
in Portainer, then redeploy.

### ETL fails with database connection error

**Cause:** `DATABASE_URL` is missing, malformed, or the Neon project is paused.

**Fix (PostgreSQL):** The URL must include `?sslmode=require`.

**Fix (MySQL):** Check that the host, port, user, password, and database name are correct.
Append `?ssl_disabled=true` if connecting to a local MySQL without SSL.

### A day of data is missing

**Cause:** The container was down during the 02:00 run, or the ETL failed partway through.

**Fix:** Trigger a manual backfill run with `ETL_DATE_OVERRIDE=YYYY-MM-DD`.
Check Portainer logs for the failure reason.

### Some groups are missing from the output

**Cause:** Only groups matching the `"Category - SLO"` / `"Category - CRO"` naming format
are included. Phone line groups `(SLO) Name` and any other format are filtered out.

**Cause 2:** The call detail fetch for those calls may have failed (network error). Check
logs for `call_detail_fetch_failed` entries.

---

## Credentials & Access

| System | Where to find credentials |
|--------|--------------------------|
| CloudTalk API | CloudTalk admin → Settings → API |
| Neon PostgreSQL | Neon console → Project → Connection Details |
| Portainer | Hosted on the Big Bang server — ask IT for the URL and login |
| GitHub repo | `https://github.com/nejcpetan/cloudtalk-analytics-etl` |
