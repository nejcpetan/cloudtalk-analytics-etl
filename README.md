# CloudTalk Analytics ETL Service

Nightly ETL service that extracts call, agent, and group statistics data from the
**CloudTalk REST API (v1.7)** and loads it into a **Neon PostgreSQL** database for
consumption by Qlik Sense BI dashboards.

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

## Local Development Setup

### Prerequisites

- Python 3.12+ installed locally (3.13 recommended)
- Access to a Neon PostgreSQL database

### 1. Clone and enter the project

```bash
git clone <repo-url>
cd cloudtalk-analytics-etl
```

### 2. Create and activate a virtual environment

```powershell
# Windows (PowerShell)
python -m venv .venv
.venv\Scripts\Activate.ps1
```

```bash
# Linux / macOS
python -m venv .venv
source .venv/bin/activate
```

### 3. Install dependencies

```bash
# Runtime only
pip install -e .

# Runtime + dev/test tools
pip install -e ".[dev]"
```

### 4. Configure environment variables

```bash
cp .env.example .env
# Edit .env and fill in:
#   CLOUDTALK_API_KEY_ID
#   CLOUDTALK_API_KEY_SECRET
#   DATABASE_URL
```

### 5. Verify the service loads correctly

```bash
python -m cloudtalk_etl
```

Expected output:
```
{"event": "cloudtalk_etl_ready", "log_level": "INFO", ...}
CloudTalk ETL ready
```

---

## Running Tests

```bash
python -m pytest tests/ -v
```

---

## Project Structure

```
cloudtalk-etl/
├── pyproject.toml          # Project metadata + dependencies
├── .env.example            # Environment variable template
├── .gitignore
├── README.md
│
├── src/cloudtalk_etl/
│   ├── __init__.py
│   ├── __main__.py         # Entry point
│   ├── config.py           # Pydantic Settings
│   ├── logging.py          # Structured JSON logging
│   ├── main.py             # ETL orchestrator (Step 1.4)
│   ├── api/                # CloudTalk HTTP client (Step 1.2)
│   ├── db/                 # Neon PostgreSQL layer (Step 1.3)
│   └── etl/                # Extract / Transform / Load (Step 1.4)
│
├── scripts/
│   ├── entrypoint.sh       # Docker entrypoint
│   └── init_db.py          # One-time DB schema init
│
└── tests/
    ├── conftest.py
    ├── test_api_client.py
    ├── test_rate_limiter.py
    ├── test_transform.py
    └── test_etl.py
```

---

## Deployment (Docker / Portainer)

See `TECHNICAL_SPEC.md` Section 7 for full Dockerfile, entrypoint, and Portainer
stack configuration instructions.

```bash
# Manual trigger via Docker exec
docker exec cloudtalk-etl python -m cloudtalk_etl

# Trigger for a specific date
docker exec -e ETL_DATE_OVERRIDE=2026-03-01 cloudtalk-etl python -m cloudtalk_etl
```

---

## Development Phases

| Phase | Step | Status |
|-------|------|--------|
| 1 — MVP | 1.1 Project scaffolding | ✅ Done |
| 1 — MVP | 1.2 API client + rate limiter | ⏳ Next |
| 1 — MVP | 1.3 Database layer | ⏳ Upcoming |
| 1 — MVP | 1.4 ETL pipeline | ⏳ Upcoming |
| 1 — MVP | 1.5 Containerisation & deployment | ⏳ Upcoming |
| 2 — CI | Conversation Intelligence | 🔜 Future |
