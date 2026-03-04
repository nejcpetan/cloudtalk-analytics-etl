# CloudTalk Analytics ETL Service

Nightly ETL service that extracts call, agent, and group statistics data from the
**CloudTalk REST API (v1.7)** and loads it into a **Neon PostgreSQL** database for
consumption by Qlik Sense BI dashboards.

Runs on a cron schedule inside Docker (via supercronic). Deployed via Portainer.

---

## Technology Stack

| Component | Version |
|-----------|---------|
| Python | 3.13 |
| httpx | 0.28.x |
| pydantic-settings | 2.7.x |
| structlog | 25.1.x |
| psycopg (v3) | 3.3.x |
| tenacity | 9.1.x |
| Target runtime | Docker (`python:3.13-slim-bookworm`) |

---

## Project Structure

```
cloudtalk-etl/
├── pyproject.toml              # Project metadata + dependencies
├── Dockerfile                  # Production container image
├── docker-compose.yml          # Local development
├── docker-compose.prod.yml     # Production (Portainer)
├── .env.example                # Environment variable template
│
├── src/cloudtalk_etl/
│   ├── __main__.py             # Entry point: python -m cloudtalk_etl
│   ├── config.py               # All env var config (pydantic-settings)
│   ├── logging.py              # Structured JSON logging (structlog)
│   ├── main.py                 # ETL orchestrator
│   │
│   ├── api/
│   │   ├── client.py           # CloudTalk HTTP client (auth, retry, pagination)
│   │   └── rate_limiter.py     # Token bucket rate limiter (50 req/min)
│   │
│   ├── db/
│   │   ├── connection.py       # Neon PostgreSQL connection
│   │   ├── schema.py           # CREATE TABLE / index DDL
│   │   └── repositories.py     # Batch upsert functions
│   │
│   └── etl/
│       ├── extract.py          # Pull data from CloudTalk API
│       ├── transform.py        # Flatten + clean raw API responses
│       └── load.py             # Write to Neon in batches
│
├── scripts/
│   ├── entrypoint.sh           # Docker entrypoint (starts supercronic)
│   └── init_db.py              # One-time DB schema initialisation
│
└── tests/
    ├── conftest.py             # Shared fixtures
    ├── test_api_client.py      # 18 tests — HTTP client + pagination
    ├── test_rate_limiter.py    # 9 tests — token bucket behaviour
    ├── test_repositories.py    # 15 tests — upsert SQL (mocked DB)
    └── test_transform.py       # 28 tests — transform + helper functions
```

---

## Local Development Setup

### Prerequisites

- Python 3.13
- Access to a Neon PostgreSQL database

### 1. Clone and enter the project

```bash
git clone <repo-url>
cd cloudtalk-analytics-etl
```

### 2. Create a virtual environment and install dependencies

```powershell
# Windows
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
# Fill in CLOUDTALK_API_KEY_ID, CLOUDTALK_API_KEY_SECRET, DATABASE_URL
```

### 4. Initialise the database schema (first time only)

```bash
python scripts/init_db.py
```

### 5. Run the ETL

```bash
# Full run (syncs yesterday's data)
python -m cloudtalk_etl

# Quick test run — fetches only 10 records per endpoint
TEST_MODE=true python -m cloudtalk_etl

# Specific date
ETL_DATE_OVERRIDE=2026-03-03 python -m cloudtalk_etl
```

---

## Running Tests

```bash
python -m pytest tests/ -v
```

70 tests, no network or database required — all HTTP calls are mocked with pytest-httpx,
all DB calls are mocked with unittest.mock.

---

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `CLOUDTALK_API_KEY_ID` | ✅ | — | CloudTalk API key ID |
| `CLOUDTALK_API_KEY_SECRET` | ✅ | — | CloudTalk API key secret |
| `DATABASE_URL` | ✅ | — | Full Neon connection string (must include `sslmode=require`) |
| `CRON_SCHEDULE` | — | `0 2 * * *` | When to run (cron syntax, consumed by Docker) |
| `LOG_LEVEL` | — | `INFO` | Log verbosity (`DEBUG`, `INFO`, `WARNING`) |
| `RATE_LIMIT_RPM` | — | `50` | API requests per minute (max 55) |
| `ETL_DATE_OVERRIDE` | — | yesterday | Sync a specific date (`YYYY-MM-DD`) — useful for backfills |
| `TEST_MODE` | — | `false` | Limit to 1 page / 10 records per endpoint for quick dry runs |
| `TZ` | — | — | Timezone for cron schedule (e.g. `Europe/Ljubljana`) |

---

## Database Schema

| Table | Key | Description |
|-------|-----|-------------|
| `calls` | `id` (BIGINT) | One row per call |
| `agents` | `(id, sync_date)` | Daily agent snapshot |
| `group_stats_daily` | `(group_id, sync_date)` | Daily group statistics |
| `call_intelligence` | `call_id` (FK → calls) | Phase 2: conversation intelligence |

All upserts use `ON CONFLICT DO UPDATE` — safe to re-run for the same date.

---

## Deployment (Portainer)

### First time

1. Push code to GitHub
2. In Portainer → **Stacks** → **Add stack** → select **Repository**
3. Set repository URL and compose path: `docker-compose.prod.yml`
4. Add environment variables (see table above)
5. Click **Deploy the stack**

### Manual trigger

```bash
# Sync yesterday's data
docker exec cloudtalk-etl python -m cloudtalk_etl

# Sync a specific date
docker exec -e ETL_DATE_OVERRIDE=2026-03-03 cloudtalk-etl python -m cloudtalk_etl
```

### Redeploy after a code change

In Portainer → **Stacks** → `cloudtalk-etl` → **Pull and redeploy**

---

## Development Status

| Phase | Step | Status |
|-------|------|--------|
| 1 — MVP | 1.1 Project scaffolding | ✅ Done |
| 1 — MVP | 1.2 API client + rate limiter | ✅ Done |
| 1 — MVP | 1.3 Database layer | ✅ Done |
| 1 — MVP | 1.4 ETL pipeline | ✅ Done |
| 1 — MVP | 1.5 Containerisation & deployment | ✅ Done |
| 2 — CI | Conversation Intelligence | 🔜 Future |
