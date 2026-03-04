# CloudTalk Analytics ETL Service

Nightly ETL service that extracts call, agent, and group statistics data from the
**CloudTalk REST API (v1.7)** and loads it into a **Neon PostgreSQL** database for
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
6. [Local Development Setup](#local-development-setup)
7. [Running Tests](#running-tests)
8. [Docker / Deployment](#docker--deployment)
9. [Operations — Day to Day](#operations--day-to-day)
10. [Backfilling Historical Data](#backfilling-historical-data)
11. [Troubleshooting](#troubleshooting)
12. [Future Work](#future-work)

---

## How It Works

### Big picture

Every night at 02:00 the container wakes up and runs a Python ETL pipeline that:

1. **Extracts** the previous day's calls, agents, and group stats from the CloudTalk API
2. **Transforms** the raw JSON into clean, flat rows (type coercion, null handling, status derivation)
3. **Loads** the rows into Neon PostgreSQL using `INSERT ... ON CONFLICT DO UPDATE` upserts

The job syncs **yesterday** by default (e.g. running at 02:00 on March 5th syncs all of March 4th).
This guarantees a complete day is always captured before the job runs.

### What data is captured

| Data | API endpoint | Frequency | Notes |
|------|-------------|-----------|-------|
| Calls (CDRs) | `/calls` | Daily | Every call with status, duration, agent, contact |
| Agents | `/agents` | Daily snapshot | Agent list with availability and extension info |
| Group statistics | `/statistic/groups` | Daily snapshot | Queue-level KPIs (answered, abandon rate, wait times) |

### Call status logic

CloudTalk does not provide a `missed/answered` field directly. The ETL derives it:
- If `answered_at` is a valid timestamp → `answered`
- If `answered_at` is null or `"0"` → `missed`

### Agent-to-call linkage

Each call record from the API contains an embedded `Agent` object. The ETL extracts
`agent_id` and `agent_name` directly onto the `calls` row, so no join is needed for
basic agent attribution.

Calls where `agent_id IS NULL` are calls that were answered by the IVR system but
where no human agent picked up (caller navigated the menu then hung up). This is
expected and not a bug — CloudTalk itself reports no agent for these.

### Rate limiting and retries

CloudTalk's API enforces a 60 req/min limit. The ETL uses a **token bucket rate
limiter** set to 50 req/min (a safety margin). On top of that, all API calls are
wrapped with **tenacity** retries: up to 5 attempts with exponential backoff + jitter,
handling transient 5xx errors and 429 rate-limit responses.

### Upserts, not deletes

All writes use `ON CONFLICT DO UPDATE`. This means:
- Re-running the ETL for the same date is always safe — no duplicate rows
- Historical data is never destroyed by a re-run
- Calls have a single PK (`id`); agents and group stats use composite PKs `(id, sync_date)` and `(group_id, sync_date)` to preserve daily history

---

## Technology Stack

| Component | Version | Purpose |
|-----------|---------|---------|
| Python | 3.13 | Runtime |
| httpx | 0.28.x | Sync HTTP client for CloudTalk API |
| pydantic-settings | 2.7.x | Typed config from env vars |
| structlog | 25.1.x | Structured JSON logging to stdout |
| psycopg (v3) | 3.3.x | PostgreSQL driver (Neon, SSL required) |
| tenacity | 9.1.x | Retry logic with exponential backoff |
| supercronic | 0.2.33 | Cron scheduler inside Docker |
| Docker base image | `python:3.13-slim-bookworm` | Minimal production image |

---

## Project Structure

```
cloudtalk-etl/
├── pyproject.toml              # Project metadata + dependencies
├── Dockerfile                  # Production container image
├── docker-compose.yml          # Local development compose
├── docker-compose.prod.yml     # Production compose (used by Portainer)
├── .env.example                # Environment variable template — copy to .env
├── .gitattributes              # Forces LF line endings on .sh files (Windows safety)
│
├── src/cloudtalk_etl/
│   ├── __main__.py             # Entry point: python -m cloudtalk_etl
│   ├── config.py               # All env var config (pydantic-settings)
│   ├── logging.py              # Structured JSON logging setup
│   ├── main.py                 # ETL orchestrator — calls extract/transform/load
│   │
│   ├── api/
│   │   ├── client.py           # CloudTalk HTTP client (auth, retry, pagination)
│   │   └── rate_limiter.py     # Token bucket rate limiter (50 req/min)
│   │
│   ├── db/
│   │   ├── connection.py       # Neon PostgreSQL connection (SSL)
│   │   ├── schema.py           # CREATE TABLE + index DDL; also runs ALTER TABLE
│   │   │                       # migrations on startup so schema stays up to date
│   │   └── repositories.py     # Batch upsert functions (calls, agents, groups)
│   │
│   └── etl/
│       ├── extract.py          # Pull data from CloudTalk API with pagination
│       ├── transform.py        # Flatten + clean raw API responses
│       └── load.py             # Write to Neon via repositories
│
├── scripts/
│   ├── entrypoint.sh           # Docker entrypoint: starts supercronic or runs once
│   └── init_db.py              # One-time DB schema initialisation (legacy helper)
│
└── tests/
    ├── conftest.py             # Shared fixtures (sample raw call record)
    ├── test_api_client.py      # HTTP client + pagination (network mocked)
    ├── test_rate_limiter.py    # Token bucket behaviour
    ├── test_repositories.py    # Upsert SQL (DB mocked)
    └── test_transform.py       # Transform logic + helper functions
```

---

## Database Schema

The database lives on **Neon PostgreSQL**. Tables are created automatically when the
ETL container first starts — you do not need to run any SQL manually.

Schema migrations (new columns added to existing tables) are also applied automatically
on each container startup via `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` in `schema.py`.

### `calls`

One row per call. Primary key is the CloudTalk call ID.

| Column | Type | Description |
|--------|------|-------------|
| `id` | BIGINT PK | CloudTalk call ID |
| `call_type` | TEXT | `incoming`, `outgoing`, `internal` |
| `call_status` | TEXT | `answered` or `missed` (derived by ETL) |
| `call_date` | DATE | Date of the call (derived from `started_at`) |
| `started_at` | TIMESTAMPTZ | When the call started |
| `answered_at` | TIMESTAMPTZ | When an agent answered (null if missed) |
| `ended_at` | TIMESTAMPTZ | When the call ended |
| `billsec` | INTEGER | Billable seconds (actual talk time) |
| `talking_time` | INTEGER | Talk time in seconds |
| `waiting_time` | INTEGER | Time in queue before answer |
| `wrapup_time` | INTEGER | After-call work time |
| `agent_id` | TEXT | ID of the agent who handled the call (null = IVR/no agent) |
| `agent_name` | TEXT | Full name of the agent (null = IVR/no agent) |
| `user_id` | TEXT | Same as agent_id (raw field from API — prefer `agent_id`) |
| `public_external` | TEXT | External phone number |
| `public_internal` | TEXT | Internal extension |
| `country_code` | TEXT | Country of the external number |
| `recorded` | BOOLEAN | Whether the call was recorded |
| `is_voicemail` | BOOLEAN | Whether it went to voicemail |
| `is_redirected` | BOOLEAN | Whether the call was redirected |
| `redirected_from` | TEXT | Number it was redirected from |
| `recording_link` | TEXT | URL to the call recording |
| `contact_id` | TEXT | CloudTalk contact ID |
| `contact_name` | TEXT | Contact display name |
| `contact_company` | TEXT | Contact company name |
| `synced_at` | TIMESTAMPTZ | When this row was last written by the ETL |

### `agents`

Daily snapshot — one row per agent per day. Composite PK `(id, sync_date)`.

| Column | Type | Description |
|--------|------|-------------|
| `id` | TEXT | CloudTalk agent ID |
| `sync_date` | DATE | Date of the snapshot |
| `firstname` | TEXT | First name |
| `lastname` | TEXT | Last name |
| `fullname` | TEXT | Concatenated full name |
| `email` | TEXT | Agent email address |
| `availability_status` | TEXT | Online/offline/away status at time of sync |
| `extension` | TEXT | Internal extension number |
| `default_number` | TEXT | Default outbound number |
| `associated_numbers` | TEXT[] | All numbers associated with this agent |

### `group_stats_daily`

Daily snapshot of queue/group KPIs. Composite PK `(group_id, sync_date)`.

| Column | Type | Description |
|--------|------|-------------|
| `group_id` | INTEGER | CloudTalk group ID |
| `group_name` | TEXT | Group display name |
| `sync_date` | DATE | Date of the snapshot |
| `operators` | INTEGER | Number of agents in the group |
| `answered` | INTEGER | Calls answered |
| `unanswered` | INTEGER | Calls not answered |
| `abandon_rate` | REAL | Abandonment rate (0.0–1.0) |
| `avg_waiting_time` | INTEGER | Average queue wait time (seconds) |
| `max_waiting_time` | INTEGER | Maximum queue wait time (seconds) |
| `avg_call_duration` | INTEGER | Average call duration (seconds) |
| `rt_waiting_queue` | INTEGER | Real-time: calls currently in queue |
| `rt_avg_waiting_time` | INTEGER | Real-time: average wait time |
| `rt_max_waiting_time` | INTEGER | Real-time: max wait time |
| `rt_avg_abandonment_time` | INTEGER | Real-time: average abandonment time |

### `call_intelligence` (Phase 2 — not yet populated)

Linked to `calls` via FK. Will hold AI transcription, sentiment, topics, and smart notes
from CloudTalk's Conversation Intelligence API when that phase is implemented.

---

## Environment Variables

Copy `.env.example` to `.env` and fill in the required values.

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `CLOUDTALK_API_KEY_ID` | ✅ | — | CloudTalk API key ID. Generate in CloudTalk → Settings → API |
| `CLOUDTALK_API_KEY_SECRET` | ✅ | — | CloudTalk API key secret |
| `DATABASE_URL` | ✅ | — | Full Neon connection string. Must include `sslmode=require`. Get it from Neon console → Connection Details → Connection string |
| `CRON_SCHEDULE` | — | `0 2 * * *` | When the ETL runs (cron syntax). Default = 02:00 every night. Consumed by Docker/supercronic, not Python |
| `LOG_LEVEL` | — | `INFO` | Log verbosity. `DEBUG` for verbose, `INFO` for normal, `WARNING` for quiet |
| `RATE_LIMIT_RPM` | — | `50` | API requests per minute. CloudTalk's limit is 60 — keep this at 50 or below |
| `ETL_DATE_OVERRIDE` | — | yesterday | Force a specific sync date in `YYYY-MM-DD` format. Used for backfills |
| `TEST_MODE` | — | `false` | If `true`, fetches only the first page (10 records) from each endpoint. Use for smoke tests |
| `TZ` | — | — | Timezone for the cron schedule. Set to `Europe/Ljubljana` in both compose files |

---

## Local Development Setup

### Prerequisites

- Python 3.13
- Access to the Neon PostgreSQL database (get the connection string from the Neon console)
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
```

### 4. Run the ETL

```bash
# Full run — syncs yesterday's data into Neon
python -m cloudtalk_etl

# Test run — fetches only 10 records per endpoint (no DB writes skipped, just less data)
TEST_MODE=true python -m cloudtalk_etl

# Sync a specific date (useful for backfills or re-runs)
ETL_DATE_OVERRIDE=2026-03-03 python -m cloudtalk_etl
```

---

## Running Tests

Tests are fully offline — no network calls, no database. All HTTP and DB interactions
are mocked.

```bash
python -m pytest tests/ -v
```

Expected output: **73 tests, all passing**.

To run a specific test file:

```bash
python -m pytest tests/test_transform.py -v
```

---

## Docker / Deployment

### How the container works

1. The container starts and runs `entrypoint.sh`
2. The entrypoint writes a crontab to `/tmp/crontab` using the `CRON_SCHEDULE` env var
3. `supercronic` reads the crontab and runs `python -m cloudtalk_etl` on schedule
4. Each run logs structured JSON to stdout (visible in Portainer logs)
5. Docker's `init: true` (tini) is set in both compose files — this is required so
   supercronic does not run as PID 1, which causes it to crash in slim containers

### Building the image locally (Linux/WSL)

```bash
docker build -t cloudtalk-etl .
```

### Running once manually (Linux/WSL)

```bash
# Sync yesterday's data
docker run --rm --env-file .env -e TZ=Europe/Ljubljana cloudtalk-etl run

# Sync a specific date
docker run --rm --env-file .env -e TZ=Europe/Ljubljana -e ETL_DATE_OVERRIDE=2026-03-03 cloudtalk-etl run

# Test mode (10 records only)
docker run --rm --env-file .env -e TZ=Europe/Ljubljana -e TEST_MODE=true cloudtalk-etl run
```

### Deploying to Portainer (first time)

1. Push code to GitHub (`git push`)
2. Open Portainer → **Stacks** → **Add stack**
3. Select **Repository** as the build method
4. Set:
   - Repository URL: `https://github.com/nejcpetan/cloudtalk-analytics-etl`
   - Compose path: `docker-compose.prod.yml`
5. Add all required environment variables (see table above)
6. Click **Deploy the stack**

The container will start, connect to Neon, apply any pending schema migrations, and
then wait for the next scheduled run.

### Redeploying after a code change

1. `git push` the changes
2. In Portainer → **Stacks** → `cloudtalk-etl` → **Pull and redeploy**

Portainer will rebuild the image from the latest commit and restart the container.
The new schema migrations (if any) will be applied automatically on startup.

---

## Operations — Day to Day

### Checking logs in Portainer

Portainer → **Containers** → `cloudtalk-etl` → **Logs**

Each ETL run produces structured JSON log lines. A successful run looks like:

```json
{"event": "etl_started",       "sync_date": "2026-03-04", ...}
{"event": "database_connected", ...}
{"event": "schema_ensured",    ...}
{"event": "calls_extracted",   "count": 724, ...}
{"event": "calls_transformed", "count": 724, ...}
{"event": "calls_upserted",    "count": 500, ...}
{"event": "calls_upserted",    "count": 224, ...}
{"event": "agents_extracted",  "count": 42, ...}
{"event": "agents_upserted",   "count": 42, ...}
{"event": "group_stats_upserted", "count": 7, ...}
{"event": "etl_completed",     "sync_date": "2026-03-04", "calls_synced": 724, "duration_seconds": 2.93, ...}
```

### Triggering a manual run without restarting the container

In Portainer → **Containers** → `cloudtalk-etl` → **Exec** → open a console, then:

```bash
python -m cloudtalk_etl
```

Or from a machine that has Docker access:

```bash
docker exec cloudtalk-etl python -m cloudtalk_etl
```

### Checking what's in the database

Connect to Neon via the Neon console SQL editor or any PostgreSQL client using the
`DATABASE_URL`. Useful queries:

```sql
-- How many calls per day?
SELECT call_date, COUNT(*) FROM calls GROUP BY call_date ORDER BY call_date DESC;

-- Calls per agent for a specific day
SELECT agent_name, COUNT(*) AS calls
FROM calls
WHERE call_date = '2026-03-04' AND agent_id IS NOT NULL
GROUP BY agent_name
ORDER BY calls DESC;

-- Calls with no agent (IVR/abandoned)
SELECT COUNT(*) FROM calls WHERE agent_id IS NULL;

-- Latest sync timestamps
SELECT MAX(synced_at) FROM calls;
SELECT MAX(synced_at) FROM agents;
```

---

## Backfilling Historical Data

If you need to (re-)sync data for a date range — for example after a bug fix or a
missed run — use `ETL_DATE_OVERRIDE` in a loop from WSL or Linux:

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

---

## Troubleshooting

### Container exits immediately / supercronic crash

**Symptom:** Container shows `Failed to fork exec: no such file or directory` or exits
right after starting.

**Cause:** `supercronic` crashes when it runs as PID 1 in a slim container because it
tries to act as a zombie reaper and fails.

**Fix:** Both compose files have `init: true` which injects Docker's `tini` as PID 1.
Make sure this is present in the compose file being used.

### CRLF line ending errors in entrypoint.sh

**Symptom:** Container fails with `/entrypoint.sh: not found` or similar.

**Cause:** Git on Windows may have committed the shell script with Windows CRLF
(`\r\n`) line endings. Bash does not understand these.

**Fix:** The Dockerfile strips CRLF at build time (`sed -i 's/\r$//' /entrypoint.sh`).
The `.gitattributes` file forces LF in the repo for all `.sh` files. If you're seeing
this, check that `.gitattributes` is committed and pull the latest image.

### ETL fails with authentication error (401)

**Cause:** `CLOUDTALK_API_KEY_ID` or `CLOUDTALK_API_KEY_SECRET` is wrong or expired.

**Fix:** Regenerate the API key in CloudTalk → Settings → API and update the env var
in Portainer (redeploy the stack after updating).

### ETL fails with database connection error

**Cause:** `DATABASE_URL` is missing, malformed, or the Neon database is paused/deleted.

**Fix:** Check the URL in Portainer matches exactly what Neon shows in Connection
Details. The URL must include `?sslmode=require` at the end.

### A day of data is missing

**Cause:** The container was down during the 02:00 run, or the ETL failed.

**Fix:** Trigger a manual backfill run with `ETL_DATE_OVERRIDE=YYYY-MM-DD` (see
Backfilling section above). Check Portainer logs for the failure reason.

### Calls show `agent_id = NULL` unexpectedly

**Cause:** These are genuinely unassigned calls — the caller interacted with the IVR
but no human agent ever picked up. CloudTalk itself does not assign an agent to these.
Average `billsec` for these calls is ~46 seconds vs ~240 seconds for agent-handled calls.
This is expected and not a data problem.

---

## Future Work

### Phase 2 — Conversation Intelligence

The `call_intelligence` table is already created in the schema (but not populated).
When CloudTalk's `enable_conversation_intelligence` flag is enabled on the account,
this phase would:

1. After each call is synced, check if a transcription is available
2. Fetch the transcript, sentiment, topics, and smart notes from the CloudTalk API
3. Store them in `call_intelligence` (FK → `calls.id`)

The config setting `ENABLE_CONVERSATION_INTELLIGENCE=true` is already wired into
`config.py` — it just needs the extract/transform/load implementation.

---

## Credentials & Access

| System | Where to find credentials |
|--------|--------------------------|
| CloudTalk API | CloudTalk admin → Settings → API |
| Neon PostgreSQL | Neon console → Project → Connection Details |
| Portainer | Hosted on the Big Bang server — ask IT for the URL and login |
| GitHub repo | `https://github.com/nejcpetan/cloudtalk-analytics-etl` |
