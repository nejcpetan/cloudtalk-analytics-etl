# CloudTalk Analytics ETL Service — Technical Specification

## Document Info

| Field | Value |
|-------|-------|
| **Project Name** | CloudTalk Analytics ETL Service |
| **Codename** | `cloudtalk-etl` |
| **Version** | 1.0 |
| **Created** | March 2026 |
| **Language** | Python 3.13 |
| **Target Runtime** | Docker (Linux amd64) |
| **Database** | Neon PostgreSQL (v16+) |

---

## 1. Technology Stack

### 1.1 Runtime & Language

| Component | Version | Rationale |
|-----------|---------|-----------|
| Python | 3.13 | Latest stable, excellent for ETL workloads |
| Docker | 24+ | Container runtime |
| Alpine / Debian Slim | Latest | Base image — prefer `python:3.13-slim-bookworm` for psycopg compatibility |

### 1.2 Python Dependencies

All dependencies should be pinned in `pyproject.toml` with exact versions for reproducibility.

| Package | Version (as of March 2026) | Purpose |
|---------|---------------------------|---------|
| `httpx` | `0.28.1` | Modern HTTP client with sync/async support, HTTP/2, connection pooling |
| `tenacity` | `9.1.4` | Retry logic with exponential backoff, jitter, and composable strategies |
| `psycopg[binary]` | `3.3.3` | Psycopg 3 — modern PostgreSQL adapter for Python. Use binary extras for Docker simplicity |
| `python-dotenv` | `1.1.0` | Load `.env` files in development |
| `pydantic` | `2.10+` | Data validation and settings management |
| `structlog` | `25.1+` | Structured JSON logging |

**Dev Dependencies:**

| Package | Purpose |
|---------|---------|
| `pytest` | Testing framework |
| `pytest-httpx` | Mock httpx requests in tests |
| `ruff` | Linting and formatting (replaces black + flake8 + isort) |

### 1.3 Infrastructure

| Component | Provider | Details |
|-----------|----------|---------|
| Database | Neon (neon.tech) | Serverless PostgreSQL, SSL required |
| Container Orchestration | Portainer | Stack deploy from Git repo |
| Scheduling | cron (inside container) | Runs via `supercronic` or standard `cron` daemon |
| Source Control | GitHub/GitLab | Git repo for CI/CD |

---

## 2. Project Structure

```
cloudtalk-etl/
├── pyproject.toml              # Project metadata + dependencies
├── Dockerfile                  # Production container image
├── docker-compose.yml          # Local development
├── .env.example                # Template for environment variables
├── .gitignore
├── README.md
│
├── src/
│   └── cloudtalk_etl/
│       ├── __init__.py
│       ├── __main__.py         # Entry point: `python -m cloudtalk_etl`
│       ├── config.py           # Pydantic Settings — all env vars
│       ├── logging.py          # Structured logging setup
│       ├── main.py             # ETL orchestrator
│       │
│       ├── api/
│       │   ├── __init__.py
│       │   ├── client.py       # CloudTalk API client with rate limiting
│       │   ├── rate_limiter.py # Token bucket rate limiter (50 req/min)
│       │   └── models.py       # Pydantic models for API responses
│       │
│       ├── db/
│       │   ├── __init__.py
│       │   ├── connection.py   # Neon PostgreSQL connection management
│       │   ├── schema.py       # Table creation / migration SQL
│       │   └── repositories.py # Insert/upsert functions per table
│       │
│       └── etl/
│           ├── __init__.py
│           ├── extract.py      # Data extraction from CloudTalk API
│           ├── transform.py    # Data cleaning and transformation
│           └── load.py         # Data loading into Neon
│
├── scripts/
│   ├── entrypoint.sh           # Docker entrypoint (starts cron + keeps alive)
│   └── init_db.py              # One-time database initialization script
│
└── tests/
    ├── conftest.py
    ├── test_api_client.py
    ├── test_rate_limiter.py
    ├── test_transform.py
    └── test_etl.py
```

---

## 3. Configuration

Use Pydantic `BaseSettings` for type-safe configuration from environment variables.

```python
# src/cloudtalk_etl/config.py
from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    # CloudTalk API
    cloudtalk_api_key_id: str
    cloudtalk_api_key_secret: str
    cloudtalk_api_base_url: str = "https://my.cloudtalk.io/api"

    # Database
    database_url: str  # Full Neon connection string with sslmode=require

    # ETL Settings
    rate_limit_rpm: int = Field(default=50, ge=1, le=55)
    log_level: str = "INFO"
    etl_date_override: str | None = None  # YYYY-MM-DD format, None = yesterday
    enable_conversation_intelligence: bool = False

    # Retry settings
    max_retries: int = 5
    retry_base_wait: float = 2.0  # seconds
    retry_max_wait: float = 60.0  # seconds

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}
```

---

## 4. CloudTalk API Client

### 4.1 HTTP Client Setup

Use `httpx.Client` (synchronous) with custom transport for simplicity. Async is unnecessary for a nightly batch job.

```python
# src/cloudtalk_etl/api/client.py
import httpx
import time
import structlog
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential_jitter,
    retry_if_exception_type,
    before_sleep_log,
)

logger = structlog.get_logger()


class CloudTalkAPIError(Exception):
    """Base exception for CloudTalk API errors."""
    pass


class CloudTalkRateLimitError(CloudTalkAPIError):
    """Raised when rate limited (HTTP 429)."""
    def __init__(self, reset_time: float | None = None):
        self.reset_time = reset_time
        super().__init__(f"Rate limited. Reset at: {reset_time}")


class CloudTalkServerError(CloudTalkAPIError):
    """Raised on 5xx responses."""
    pass


class CloudTalkClient:
    def __init__(self, api_key_id: str, api_key_secret: str,
                 base_url: str, rate_limiter):
        self._auth = (api_key_id, api_key_secret)
        self._base_url = base_url.rstrip("/")
        self._rate_limiter = rate_limiter
        self._client = httpx.Client(
            timeout=httpx.Timeout(30.0, connect=10.0),
            headers={"Accept": "application/json"},
        )

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential_jitter(initial=2, max=60, jitter=2),
        retry=retry_if_exception_type(
            (CloudTalkRateLimitError, CloudTalkServerError, httpx.TransportError)
        ),
        before_sleep=before_sleep_log(logger, "WARNING"),
        reraise=True,
    )
    def _request(self, method: str, endpoint: str,
                 params: dict | None = None) -> dict:
        """Make a rate-limited, retryable request to CloudTalk API."""
        self._rate_limiter.wait()  # Block until we have capacity

        url = f"{self._base_url}{endpoint}"
        response = self._client.request(
            method, url, params=params, auth=self._auth
        )

        # Handle rate limiting
        if response.status_code == 429:
            reset_time = response.headers.get("X-CloudTalkAPI-ResetTime")
            if reset_time:
                wait_seconds = float(reset_time) - time.time()
                if wait_seconds > 0:
                    logger.warning("rate_limited", wait_seconds=wait_seconds)
                    time.sleep(wait_seconds)
            raise CloudTalkRateLimitError(reset_time=reset_time)

        # Handle server errors (retryable)
        if response.status_code >= 500:
            raise CloudTalkServerError(
                f"Server error {response.status_code}: {response.text[:200]}"
            )

        # Handle client errors (not retryable)
        if response.status_code >= 400:
            logger.error("client_error",
                         status=response.status_code,
                         body=response.text[:500])
            response.raise_for_status()

        return response.json()

    def get_calls(self, date_from: str, date_to: str,
                  page: int = 1, limit: int = 1000) -> dict:
        """Fetch call history for a date range."""
        return self._request("GET", "/calls/index.json", params={
            "date_from": date_from,
            "date_to": date_to,
            "page": page,
            "limit": limit,
        })

    def get_agents(self, page: int = 1, limit: int = 1000) -> dict:
        """Fetch all agents."""
        return self._request("GET", "/agents/index.json", params={
            "page": page,
            "limit": limit,
        })

    def get_group_stats(self) -> dict:
        """Fetch realtime group statistics."""
        return self._request("GET", "/statistics/realtime/groups.json")

    def get_all_pages(self, fetch_fn, **kwargs) -> list:
        """Generic paginator. Calls fetch_fn repeatedly until all pages retrieved."""
        all_data = []
        page = 1

        while True:
            response = fetch_fn(page=page, **kwargs)
            response_data = response.get("responseData", {})
            data = response_data.get("data", [])
            all_data.extend(data)

            page_count = response_data.get("pageCount", 1)
            if page >= page_count:
                break
            page += 1

        return all_data

    def close(self):
        self._client.close()
```

### 4.2 Rate Limiter

Implement a simple token bucket rate limiter. This runs synchronously since the ETL is a sequential batch process.

```python
# src/cloudtalk_etl/api/rate_limiter.py
import time
import threading


class TokenBucketRateLimiter:
    """
    Token bucket rate limiter.

    Maintains a bucket of tokens that refill at a steady rate.
    Each API call consumes one token. If no tokens are available,
    the caller blocks until a token is available.

    Args:
        rate_per_minute: Maximum requests per minute (default: 50)
    """

    def __init__(self, rate_per_minute: int = 50):
        self.rate_per_minute = rate_per_minute
        self.interval = 60.0 / rate_per_minute  # seconds between tokens
        self._lock = threading.Lock()
        self._last_request_time = 0.0
        self._tokens = rate_per_minute  # Start with full bucket
        self._last_refill = time.monotonic()

    def wait(self) -> None:
        """Block until a token is available, then consume it."""
        with self._lock:
            now = time.monotonic()

            # Refill tokens based on elapsed time
            elapsed = now - self._last_refill
            new_tokens = elapsed / self.interval
            self._tokens = min(self.rate_per_minute, self._tokens + new_tokens)
            self._last_refill = now

            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return

            # No tokens available — calculate wait time
            wait_time = (1.0 - self._tokens) * self.interval
            self._tokens = 0.0

        # Sleep outside the lock
        time.sleep(wait_time)

        # After sleeping, consume token
        with self._lock:
            self._tokens = 0.0
            self._last_refill = time.monotonic()
```

---

## 5. Database Layer

### 5.1 Connection Management

```python
# src/cloudtalk_etl/db/connection.py
import psycopg
import structlog

logger = structlog.get_logger()


def get_connection(database_url: str) -> psycopg.Connection:
    """
    Create a connection to Neon PostgreSQL.

    Neon requires SSL. The connection string should include sslmode=require.
    Uses autocommit=False for transactional writes.
    """
    conn = psycopg.connect(
        database_url,
        autocommit=False,
    )
    logger.info("database_connected", server=conn.info.host)
    return conn
```

### 5.2 Database Schema

```sql
-- Schema initialization script
-- Run once via: python -m cloudtalk_etl.scripts.init_db

-- Calls table: one row per call
CREATE TABLE IF NOT EXISTS calls (
    id                  BIGINT PRIMARY KEY,        -- CloudTalk call ID
    call_type           TEXT NOT NULL,              -- incoming, outgoing, internal
    billsec             INTEGER DEFAULT 0,          -- Billed seconds
    talking_time        INTEGER DEFAULT 0,          -- Talk time in seconds
    waiting_time        INTEGER DEFAULT 0,          -- Wait time in seconds
    wrapup_time         INTEGER DEFAULT 0,          -- Wrap-up time in seconds
    public_external     TEXT,                        -- External phone number
    public_internal     TEXT,                        -- Agent's internal number
    country_code        TEXT,                        -- Country code of caller
    recorded            BOOLEAN DEFAULT FALSE,
    is_voicemail        BOOLEAN DEFAULT FALSE,
    is_redirected       BOOLEAN DEFAULT FALSE,
    redirected_from     TEXT,
    user_id             TEXT,                        -- Agent ID
    started_at          TIMESTAMPTZ,
    answered_at         TIMESTAMPTZ,                 -- NULL if missed
    ended_at            TIMESTAMPTZ,
    recording_link      TEXT,
    -- Derived fields
    call_status         TEXT,                        -- 'answered' or 'missed' (derived from answered_at)
    call_date           DATE,                        -- Derived from started_at for easy filtering
    -- Contact info (denormalized for Qlik simplicity)
    contact_id          TEXT,
    contact_name        TEXT,
    contact_company     TEXT,
    -- Metadata
    synced_at           TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for common Qlik queries
CREATE INDEX IF NOT EXISTS idx_calls_call_date ON calls (call_date);
CREATE INDEX IF NOT EXISTS idx_calls_user_id ON calls (user_id);
CREATE INDEX IF NOT EXISTS idx_calls_call_type ON calls (call_type);
CREATE INDEX IF NOT EXISTS idx_calls_call_status ON calls (call_status);
CREATE INDEX IF NOT EXISTS idx_calls_started_at ON calls (started_at);

-- Agents table: one row per agent per sync date
CREATE TABLE IF NOT EXISTS agents (
    id                  TEXT NOT NULL,               -- CloudTalk agent ID
    sync_date           DATE NOT NULL,               -- Date of this snapshot
    firstname           TEXT,
    lastname            TEXT,
    fullname            TEXT,                         -- Combined first + last
    email               TEXT,
    availability_status TEXT,                         -- online, offline, busy, etc.
    extension           TEXT,
    default_number      TEXT,
    associated_numbers  TEXT[],                       -- PostgreSQL array
    synced_at           TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (id, sync_date)
);

CREATE INDEX IF NOT EXISTS idx_agents_sync_date ON agents (sync_date);

-- Group statistics: one row per group per sync date
CREATE TABLE IF NOT EXISTS group_stats_daily (
    group_id            INTEGER NOT NULL,
    group_name          TEXT NOT NULL,
    sync_date           DATE NOT NULL,
    operators           INTEGER DEFAULT 0,           -- Number of agents in group
    answered            INTEGER DEFAULT 0,           -- Calls answered
    unanswered          INTEGER DEFAULT 0,           -- Calls unanswered
    abandon_rate        REAL DEFAULT 0.0,            -- Abandon rate percentage
    avg_waiting_time    INTEGER DEFAULT 0,           -- Average wait (seconds)
    max_waiting_time    INTEGER DEFAULT 0,           -- Max wait (seconds)
    avg_call_duration   INTEGER DEFAULT 0,           -- Average call duration (seconds)
    -- Realtime snapshot at sync time
    rt_waiting_queue      INTEGER DEFAULT 0,
    rt_avg_waiting_time   INTEGER DEFAULT 0,
    rt_max_waiting_time   INTEGER DEFAULT 0,
    rt_avg_abandonment_time INTEGER DEFAULT 0,
    synced_at           TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (group_id, sync_date)
);

CREATE INDEX IF NOT EXISTS idx_group_stats_sync_date ON group_stats_daily (sync_date);

-- Phase 2: Conversation Intelligence (created but not populated until enabled)
CREATE TABLE IF NOT EXISTS call_intelligence (
    call_id             BIGINT PRIMARY KEY REFERENCES calls(id),
    summary             TEXT,
    overall_sentiment   JSONB,                       -- Full sentiment object
    talk_listen_ratio   JSONB,                       -- Full ratio object
    topics              JSONB,                       -- Array of topics
    transcription       JSONB,                       -- Full transcription object
    smart_notes         TEXT,
    synced_at           TIMESTAMPTZ DEFAULT NOW()
);
```

### 5.3 Repository Layer (Upserts)

```python
# src/cloudtalk_etl/db/repositories.py
import psycopg
import structlog
from datetime import date

logger = structlog.get_logger()


def upsert_calls(conn: psycopg.Connection, calls: list[dict]) -> int:
    """
    Batch upsert call records.

    Uses PostgreSQL ON CONFLICT to handle idempotent re-runs.
    Returns the number of rows upserted.
    """
    if not calls:
        return 0

    query = """
        INSERT INTO calls (
            id, call_type, billsec, talking_time, waiting_time, wrapup_time,
            public_external, public_internal, country_code, recorded,
            is_voicemail, is_redirected, redirected_from, user_id,
            started_at, answered_at, ended_at, recording_link,
            call_status, call_date,
            contact_id, contact_name, contact_company, synced_at
        ) VALUES (
            %(id)s, %(call_type)s, %(billsec)s, %(talking_time)s,
            %(waiting_time)s, %(wrapup_time)s, %(public_external)s,
            %(public_internal)s, %(country_code)s, %(recorded)s,
            %(is_voicemail)s, %(is_redirected)s, %(redirected_from)s,
            %(user_id)s, %(started_at)s, %(answered_at)s, %(ended_at)s,
            %(recording_link)s, %(call_status)s, %(call_date)s,
            %(contact_id)s, %(contact_name)s, %(contact_company)s, NOW()
        )
        ON CONFLICT (id) DO UPDATE SET
            call_type = EXCLUDED.call_type,
            billsec = EXCLUDED.billsec,
            talking_time = EXCLUDED.talking_time,
            waiting_time = EXCLUDED.waiting_time,
            wrapup_time = EXCLUDED.wrapup_time,
            public_external = EXCLUDED.public_external,
            public_internal = EXCLUDED.public_internal,
            recorded = EXCLUDED.recorded,
            answered_at = EXCLUDED.answered_at,
            ended_at = EXCLUDED.ended_at,
            call_status = EXCLUDED.call_status,
            contact_id = EXCLUDED.contact_id,
            contact_name = EXCLUDED.contact_name,
            contact_company = EXCLUDED.contact_company,
            synced_at = NOW()
    """

    with conn.cursor() as cur:
        cur.executemany(query, calls)

    conn.commit()
    count = len(calls)
    logger.info("calls_upserted", count=count)
    return count


def upsert_agents(conn: psycopg.Connection, agents: list[dict],
                  sync_date: date) -> int:
    """Batch upsert agent snapshots for a given date."""
    if not agents:
        return 0

    query = """
        INSERT INTO agents (
            id, sync_date, firstname, lastname, fullname, email,
            availability_status, extension, default_number,
            associated_numbers, synced_at
        ) VALUES (
            %(id)s, %(sync_date)s, %(firstname)s, %(lastname)s,
            %(fullname)s, %(email)s, %(availability_status)s,
            %(extension)s, %(default_number)s,
            %(associated_numbers)s, NOW()
        )
        ON CONFLICT (id, sync_date) DO UPDATE SET
            firstname = EXCLUDED.firstname,
            lastname = EXCLUDED.lastname,
            fullname = EXCLUDED.fullname,
            email = EXCLUDED.email,
            availability_status = EXCLUDED.availability_status,
            extension = EXCLUDED.extension,
            default_number = EXCLUDED.default_number,
            associated_numbers = EXCLUDED.associated_numbers,
            synced_at = NOW()
    """

    with conn.cursor() as cur:
        cur.executemany(query, agents)

    conn.commit()
    count = len(agents)
    logger.info("agents_upserted", count=count, sync_date=str(sync_date))
    return count


def upsert_group_stats(conn: psycopg.Connection, stats: list[dict],
                       sync_date: date) -> int:
    """Batch upsert group statistics for a given date."""
    if not stats:
        return 0

    query = """
        INSERT INTO group_stats_daily (
            group_id, group_name, sync_date, operators, answered,
            unanswered, abandon_rate, avg_waiting_time, max_waiting_time,
            avg_call_duration, rt_waiting_queue, rt_avg_waiting_time,
            rt_max_waiting_time, rt_avg_abandonment_time, synced_at
        ) VALUES (
            %(group_id)s, %(group_name)s, %(sync_date)s, %(operators)s,
            %(answered)s, %(unanswered)s, %(abandon_rate)s,
            %(avg_waiting_time)s, %(max_waiting_time)s,
            %(avg_call_duration)s, %(rt_waiting_queue)s,
            %(rt_avg_waiting_time)s, %(rt_max_waiting_time)s,
            %(rt_avg_abandonment_time)s, NOW()
        )
        ON CONFLICT (group_id, sync_date) DO UPDATE SET
            group_name = EXCLUDED.group_name,
            operators = EXCLUDED.operators,
            answered = EXCLUDED.answered,
            unanswered = EXCLUDED.unanswered,
            abandon_rate = EXCLUDED.abandon_rate,
            avg_waiting_time = EXCLUDED.avg_waiting_time,
            max_waiting_time = EXCLUDED.max_waiting_time,
            avg_call_duration = EXCLUDED.avg_call_duration,
            rt_waiting_queue = EXCLUDED.rt_waiting_queue,
            rt_avg_waiting_time = EXCLUDED.rt_avg_waiting_time,
            rt_max_waiting_time = EXCLUDED.rt_max_waiting_time,
            rt_avg_abandonment_time = EXCLUDED.rt_avg_abandonment_time,
            synced_at = NOW()
    """

    with conn.cursor() as cur:
        cur.executemany(query, stats)

    conn.commit()
    count = len(stats)
    logger.info("group_stats_upserted", count=count, sync_date=str(sync_date))
    return count
```

---

## 6. ETL Pipeline

### 6.1 Orchestrator

```python
# src/cloudtalk_etl/main.py
import sys
import time
import structlog
from datetime import date, datetime, timedelta

from cloudtalk_etl.config import Settings
from cloudtalk_etl.api.client import CloudTalkClient
from cloudtalk_etl.api.rate_limiter import TokenBucketRateLimiter
from cloudtalk_etl.db.connection import get_connection
from cloudtalk_etl.db.schema import ensure_schema
from cloudtalk_etl.etl.extract import extract_calls, extract_agents, extract_group_stats
from cloudtalk_etl.etl.transform import transform_calls, transform_agents, transform_group_stats
from cloudtalk_etl.etl.load import load_calls, load_agents, load_group_stats

logger = structlog.get_logger()


def determine_sync_date(override: str | None) -> date:
    """Determine which date to sync. Default: yesterday."""
    if override:
        return date.fromisoformat(override)
    return date.today() - timedelta(days=1)


def run_etl():
    """Main ETL entry point."""
    start_time = time.monotonic()
    settings = Settings()
    sync_date = determine_sync_date(settings.etl_date_override)

    logger.info("etl_started", sync_date=str(sync_date))

    # Initialize components
    rate_limiter = TokenBucketRateLimiter(rate_per_minute=settings.rate_limit_rpm)
    api_client = CloudTalkClient(
        api_key_id=settings.cloudtalk_api_key_id,
        api_key_secret=settings.cloudtalk_api_key_secret,
        base_url=settings.cloudtalk_api_base_url,
        rate_limiter=rate_limiter,
    )
    conn = get_connection(settings.database_url)

    try:
        # Ensure database tables exist
        ensure_schema(conn)

        # === EXTRACT ===
        raw_calls = extract_calls(api_client, sync_date)
        raw_agents = extract_agents(api_client)
        raw_group_stats = extract_group_stats(api_client)

        # === TRANSFORM ===
        calls = transform_calls(raw_calls, sync_date)
        agents = transform_agents(raw_agents, sync_date)
        group_stats = transform_group_stats(raw_group_stats, sync_date)

        # === LOAD ===
        calls_count = load_calls(conn, calls)
        agents_count = load_agents(conn, agents, sync_date)
        groups_count = load_group_stats(conn, group_stats, sync_date)

        elapsed = time.monotonic() - start_time
        logger.info(
            "etl_completed",
            sync_date=str(sync_date),
            calls_synced=calls_count,
            agents_synced=agents_count,
            groups_synced=groups_count,
            duration_seconds=round(elapsed, 2),
        )

    except Exception:
        elapsed = time.monotonic() - start_time
        logger.exception("etl_failed", sync_date=str(sync_date),
                         duration_seconds=round(elapsed, 2))
        sys.exit(1)

    finally:
        api_client.close()
        conn.close()
```

### 6.2 Extract Layer

```python
# src/cloudtalk_etl/etl/extract.py
import structlog
from datetime import date

from cloudtalk_etl.api.client import CloudTalkClient

logger = structlog.get_logger()


def extract_calls(client: CloudTalkClient, sync_date: date) -> list[dict]:
    """
    Extract all calls for a given date.

    Uses date_from (start of day) and date_to (end of day) filters.
    Handles pagination automatically.
    """
    date_from = f"{sync_date} 00:00:00"
    date_to = f"{sync_date} 23:59:59"

    logger.info("extracting_calls", date_from=date_from, date_to=date_to)

    calls = client.get_all_pages(
        client.get_calls,
        date_from=date_from,
        date_to=date_to,
    )

    logger.info("calls_extracted", count=len(calls))
    return calls


def extract_agents(client: CloudTalkClient) -> list[dict]:
    """Extract all agents."""
    logger.info("extracting_agents")

    agents = client.get_all_pages(client.get_agents)

    logger.info("agents_extracted", count=len(agents))
    return agents


def extract_group_stats(client: CloudTalkClient) -> dict:
    """Extract group statistics snapshot."""
    logger.info("extracting_group_stats")

    response = client.get_group_stats()
    groups = response.get("responseData", {}).get("data", {}).get("groups", [])

    logger.info("group_stats_extracted", count=len(groups))
    return groups
```

### 6.3 Transform Layer

```python
# src/cloudtalk_etl/etl/transform.py
import structlog
from datetime import date, datetime

logger = structlog.get_logger()


def safe_int(value, default=0) -> int:
    """Safely convert a value to int."""
    try:
        return int(value) if value is not None else default
    except (ValueError, TypeError):
        return default


def safe_float(value, default=0.0) -> float:
    """Safely convert a value to float."""
    try:
        return float(value) if value is not None else default
    except (ValueError, TypeError):
        return default


def parse_timestamp(value: str | None) -> str | None:
    """Parse and validate a timestamp string. Returns None if invalid."""
    if not value or value == "" or value == "0":
        return None
    try:
        # Validate it's a parseable timestamp
        datetime.fromisoformat(value.replace("Z", "+00:00"))
        return value
    except (ValueError, AttributeError):
        return None


def transform_calls(raw_calls: list[dict], sync_date: date) -> list[dict]:
    """
    Transform raw CloudTalk call data into flat dictionaries
    ready for database insertion.
    """
    transformed = []

    for record in raw_calls:
        cdr = record.get("Cdr", {})
        contact = record.get("Contact", {})

        answered_at = parse_timestamp(cdr.get("answered_at"))
        call_status = "answered" if answered_at else "missed"

        started_at = parse_timestamp(cdr.get("started_at"))
        call_date = sync_date  # Use sync_date as fallback

        if started_at:
            try:
                dt = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
                call_date = dt.date()
            except ValueError:
                pass

        transformed.append({
            "id": safe_int(cdr.get("id")),
            "call_type": cdr.get("type", "unknown"),
            "billsec": safe_int(cdr.get("billsec")),
            "talking_time": safe_int(cdr.get("talking_time")),
            "waiting_time": safe_int(cdr.get("waiting_time")),
            "wrapup_time": safe_int(cdr.get("wrapup_time")),
            "public_external": str(cdr.get("public_external", "")) or None,
            "public_internal": str(cdr.get("public_internal", "")) or None,
            "country_code": cdr.get("country_code"),
            "recorded": bool(cdr.get("recorded", False)),
            "is_voicemail": bool(cdr.get("is_voicemail", False)),
            "is_redirected": bool(cdr.get("is_redirected", False) 
                                  if cdr.get("is_redirected") != "0" else False),
            "redirected_from": cdr.get("redirected_from") or None,
            "user_id": str(cdr.get("user_id", "")) or None,
            "started_at": started_at,
            "answered_at": answered_at,
            "ended_at": parse_timestamp(cdr.get("ended_at")),
            "recording_link": cdr.get("recording_link"),
            "call_status": call_status,
            "call_date": call_date,
            "contact_id": str(contact.get("id", "")) or None,
            "contact_name": contact.get("name") or None,
            "contact_company": contact.get("company") or None,
        })

    logger.info("calls_transformed", count=len(transformed))
    return transformed


def transform_agents(raw_agents: list[dict], sync_date: date) -> list[dict]:
    """Transform raw CloudTalk agent data."""
    transformed = []

    for record in raw_agents:
        agent = record.get("Agent", {})
        firstname = agent.get("firstname", "")
        lastname = agent.get("lastname", "")

        transformed.append({
            "id": str(agent.get("id", "")),
            "sync_date": sync_date,
            "firstname": firstname or None,
            "lastname": lastname or None,
            "fullname": f"{firstname} {lastname}".strip() or None,
            "email": agent.get("email") or None,
            "availability_status": agent.get("availability_status") or None,
            "extension": agent.get("extension") or None,
            "default_number": agent.get("default_number") or None,
            "associated_numbers": agent.get("associated_numbers", []),
        })

    logger.info("agents_transformed", count=len(transformed))
    return transformed


def transform_group_stats(raw_stats: list[dict], sync_date: date) -> list[dict]:
    """Transform raw CloudTalk group statistics."""
    transformed = []

    for group in raw_stats:
        real_time = group.get("real_time", {})

        transformed.append({
            "group_id": safe_int(group.get("id")),
            "group_name": group.get("name", "Unknown"),
            "sync_date": sync_date,
            "operators": safe_int(group.get("operators")),
            "answered": safe_int(group.get("answered")),
            "unanswered": safe_int(group.get("unanswered")),
            "abandon_rate": safe_float(group.get("abandon_rate")),
            "avg_waiting_time": safe_int(group.get("avg_waiting_time")),
            "max_waiting_time": safe_int(group.get("max_waiting_time")),
            "avg_call_duration": safe_int(group.get("avg_call_duration")),
            "rt_waiting_queue": safe_int(real_time.get("waiting_queue")),
            "rt_avg_waiting_time": safe_int(real_time.get("avg_waiting_time")),
            "rt_max_waiting_time": safe_int(real_time.get("max_waiting_time")),
            "rt_avg_abandonment_time": safe_int(
                real_time.get("avg_abandonment_time")
            ),
        })

    logger.info("group_stats_transformed", count=len(transformed))
    return transformed
```

### 6.4 Load Layer

```python
# src/cloudtalk_etl/etl/load.py
import psycopg
import structlog
from datetime import date

from cloudtalk_etl.db.repositories import (
    upsert_calls,
    upsert_agents,
    upsert_group_stats,
)

logger = structlog.get_logger()

BATCH_SIZE = 500  # Insert in batches to manage memory and transaction size


def load_calls(conn: psycopg.Connection, calls: list[dict]) -> int:
    """Load call records in batches."""
    total = 0
    for i in range(0, len(calls), BATCH_SIZE):
        batch = calls[i:i + BATCH_SIZE]
        total += upsert_calls(conn, batch)
    return total


def load_agents(conn: psycopg.Connection, agents: list[dict],
                sync_date: date) -> int:
    """Load agent records."""
    return upsert_agents(conn, agents, sync_date)


def load_group_stats(conn: psycopg.Connection, stats: list[dict],
                     sync_date: date) -> int:
    """Load group statistics."""
    return upsert_group_stats(conn, stats, sync_date)
```

---

## 7. Docker & Deployment

### 7.1 Dockerfile

```dockerfile
FROM python:3.13-slim-bookworm

# Security: create non-root user
RUN groupadd --system etl && useradd --system --gid etl etl

# Install cron (supercronic for Docker-friendly cron)
# supercronic is a cron alternative designed for containers:
# - logs to stdout/stderr (visible in docker logs)
# - no syslog dependency
# - proper signal handling for graceful shutdown
ARG SUPERCRONIC_VERSION=v0.2.33
ARG SUPERCRONIC_SHA256=71b0d58cc53f6bd72f4f2c7e935f348f9b2a4c8405de4e1a3b9e11aa93b1f7da
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
        ca-certificates \
    && curl -fsSLO "https://github.com/aptible/supercronic/releases/download/${SUPERCRONIC_VERSION}/supercronic-linux-amd64" \
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

# Create crontab file (uses CRON_SCHEDULE env var at runtime)
COPY scripts/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Create log directory
RUN mkdir -p /app/logs && chown etl:etl /app/logs

USER etl

ENTRYPOINT ["/entrypoint.sh"]
```

### 7.2 Entrypoint Script

```bash
#!/bin/bash
# scripts/entrypoint.sh

set -e

CRON_SCHEDULE="${CRON_SCHEDULE:-0 2 * * *}"

echo "CloudTalk ETL Service starting..."
echo "Schedule: ${CRON_SCHEDULE}"
echo "Log level: ${LOG_LEVEL:-INFO}"

# Generate crontab from environment variable
echo "${CRON_SCHEDULE} python -m cloudtalk_etl" > /tmp/crontab

# If first argument is "run", execute ETL immediately (for testing / manual trigger)
if [ "$1" = "run" ]; then
    echo "Running ETL immediately..."
    exec python -m cloudtalk_etl
fi

# Otherwise, start supercronic (foreground, logs to stdout)
echo "Starting cron daemon..."
exec supercronic /tmp/crontab
```

### 7.3 docker-compose.yml (Local Development)

```yaml
version: "3.8"

services:
  cloudtalk-etl:
    build: .
    container_name: cloudtalk-etl
    restart: unless-stopped
    env_file:
      - .env
    environment:
      - TZ=Europe/Ljubljana
    # Override entrypoint for development: run once and exit
    # command: run
```

### 7.4 Portainer Deployment

Deploy as a Portainer stack using the git repository:

1. In Portainer, go to **Stacks → Add Stack**
2. Select **Repository** as the build method
3. Enter the Git repository URL and branch
4. Set the Compose path to `docker-compose.yml` (or create a separate `docker-compose.prod.yml`)
5. Add all environment variables in the **Environment variables** section:
   - `CLOUDTALK_API_KEY_ID`
   - `CLOUDTALK_API_KEY_SECRET`
   - `DATABASE_URL`
   - `CRON_SCHEDULE=0 2 * * *`
   - `LOG_LEVEL=INFO`
   - `RATE_LIMIT_RPM=50`
   - `TZ=Europe/Ljubljana`
6. Deploy the stack

**To manually trigger the ETL** (e.g., for testing or re-runs):

```bash
# Via Portainer console or SSH into the server:
docker exec cloudtalk-etl python -m cloudtalk_etl

# To sync a specific date:
docker exec -e ETL_DATE_OVERRIDE=2026-03-01 cloudtalk-etl python -m cloudtalk_etl
```

---

## 8. Logging

### 8.1 Setup

```python
# src/cloudtalk_etl/logging.py
import structlog
import logging
import sys


def setup_logging(log_level: str = "INFO"):
    """Configure structured JSON logging."""

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, log_level.upper(), logging.INFO)
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=True,
    )
```

### 8.2 Example Log Output

```json
{"event": "etl_started", "sync_date": "2026-03-03", "level": "info", "timestamp": "2026-03-04T02:00:01.123Z"}
{"event": "extracting_calls", "date_from": "2026-03-03 00:00:00", "date_to": "2026-03-03 23:59:59", "level": "info", "timestamp": "2026-03-04T02:00:01.456Z"}
{"event": "calls_extracted", "count": 347, "level": "info", "timestamp": "2026-03-04T02:00:03.789Z"}
{"event": "calls_transformed", "count": 347, "level": "info", "timestamp": "2026-03-04T02:00:03.801Z"}
{"event": "calls_upserted", "count": 347, "level": "info", "timestamp": "2026-03-04T02:00:04.234Z"}
{"event": "etl_completed", "sync_date": "2026-03-03", "calls_synced": 347, "agents_synced": 45, "groups_synced": 4, "duration_seconds": 3.11, "level": "info", "timestamp": "2026-03-04T02:00:04.234Z"}
```

---

## 9. Error Handling Strategy

### 9.1 Error Classification

| Error Type | HTTP Code | Retryable | Action |
|------------|-----------|-----------|--------|
| Rate Limited | 429 | Yes | Wait for `X-CloudTalkAPI-ResetTime`, then retry |
| Server Error | 500-599 | Yes | Exponential backoff with jitter, up to 5 retries |
| Bad Request | 400 | No | Log error, skip this request |
| Unauthorized | 401 | No | Log error, fail immediately (bad credentials) |
| Forbidden | 403 | No | Log error, fail immediately |
| Not Found | 404 | No | Log warning, skip (e.g., deleted call) |
| Connection Error | N/A | Yes | Exponential backoff, up to 5 retries |
| Timeout | N/A | Yes | Exponential backoff, up to 5 retries |
| Database Error | N/A | Depends | Retry connection errors; fail on constraint violations |

### 9.2 Retry Configuration (via tenacity)

```python
# Applied to every API call via the @retry decorator on _request()

stop=stop_after_attempt(5)                      # Max 5 attempts
wait=wait_exponential_jitter(
    initial=2,                                   # First retry after ~2s
    max=60,                                      # Never wait more than 60s
    jitter=2                                     # Add 0-2s random jitter
)
retry=retry_if_exception_type(
    (CloudTalkRateLimitError,                    # 429
     CloudTalkServerError,                       # 5xx
     httpx.TransportError)                       # Network errors
)
```

### 9.3 Graceful Degradation

If one pipeline stage fails, the ETL should attempt all stages and report a summary:

- If calls extraction fails but agents succeeds, still write agents
- The overall ETL exit code should be 1 (failure) if ANY stage fails
- Each stage's success/failure should be logged independently

---

## 10. Testing Strategy

### 10.1 Unit Tests

Test transforms and utilities without any external dependencies:

```python
# tests/test_transform.py

def test_transform_calls_handles_missing_contact():
    """Calls without a Contact object should still transform cleanly."""
    raw = [{"Cdr": {"id": "1", "type": "incoming", "started_at": "2026-03-03T10:00:00Z"}}]
    result = transform_calls(raw, date(2026, 3, 3))
    assert len(result) == 1
    assert result[0]["contact_name"] is None


def test_transform_calls_derives_missed_status():
    """Calls with no answered_at should be marked as 'missed'."""
    raw = [{"Cdr": {"id": "2", "type": "incoming", "answered_at": None, "started_at": "2026-03-03T10:00:00Z"}}]
    result = transform_calls(raw, date(2026, 3, 3))
    assert result[0]["call_status"] == "missed"


def test_safe_int_handles_garbage():
    assert safe_int("abc") == 0
    assert safe_int(None) == 0
    assert safe_int("42") == 42
```

### 10.2 Integration Tests

Test API client with mocked HTTP responses:

```python
# tests/test_api_client.py

def test_pagination_fetches_all_pages(httpx_mock):
    """Verify paginator calls all pages and aggregates results."""
    # Mock page 1
    httpx_mock.add_response(json={
        "responseData": {"data": [{"id": 1}], "pageCount": 2, "pageNumber": 1}
    })
    # Mock page 2
    httpx_mock.add_response(json={
        "responseData": {"data": [{"id": 2}], "pageCount": 2, "pageNumber": 2}
    })

    results = client.get_all_pages(client.get_calls, date_from="...", date_to="...")
    assert len(results) == 2


def test_rate_limit_retry(httpx_mock):
    """Verify 429 responses trigger retry."""
    httpx_mock.add_response(status_code=429, headers={"X-CloudTalkAPI-ResetTime": "..."})
    httpx_mock.add_response(json={"responseData": {"data": []}})

    # Should succeed on second attempt
    result = client.get_agents()
    assert result is not None
```

### 10.3 Running Tests

```bash
# Run all tests
python -m pytest tests/ -v

# Run with coverage
python -m pytest tests/ --cov=cloudtalk_etl --cov-report=term-missing
```

---

## 11. pyproject.toml

```toml
[project]
name = "cloudtalk-etl"
version = "1.0.0"
description = "Nightly ETL service: CloudTalk API → Neon PostgreSQL for Qlik Sense analytics"
requires-python = ">=3.12"
dependencies = [
    "httpx>=0.28.0,<0.29",
    "tenacity>=9.1.0,<10",
    "psycopg[binary]>=3.3.0,<4",
    "pydantic>=2.10,<3",
    "pydantic-settings>=2.7,<3",
    "structlog>=25.1,<26",
    "python-dotenv>=1.1,<2",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0",
    "pytest-httpx>=0.35",
    "ruff>=0.9",
]

[build-system]
requires = ["setuptools>=75"]
build-backend = "setuptools.backends._legacy:_Backend"

[tool.setuptools.packages.find]
where = ["src"]

[tool.ruff]
target-version = "py313"
line-length = 100
```

---

## 12. .env.example

```bash
# CloudTalk API credentials
# Found in: CloudTalk Dashboard → Account → Settings → API Keys
CLOUDTALK_API_KEY_ID=your_api_key_id_here
CLOUDTALK_API_KEY_SECRET=your_api_key_secret_here

# Neon PostgreSQL connection string
# Found in: Neon Dashboard → Connection Details → Connection String
DATABASE_URL=postgresql://username:password@ep-example-name-12345.eu-central-1.aws.neon.tech/cloudtalk_analytics?sslmode=require

# ETL Configuration
CRON_SCHEDULE=0 2 * * *
LOG_LEVEL=INFO
RATE_LIMIT_RPM=50
TZ=Europe/Ljubljana

# Optional overrides
# ETL_DATE_OVERRIDE=2026-03-01
# ENABLE_CONVERSATION_INTELLIGENCE=false
```

---

## 13. Implementation Notes for AI Agent

### 13.1 Build Order

Follow this exact sequence to build the project incrementally and testably:

1. Create project structure and `pyproject.toml` first
2. Implement `config.py` (Pydantic Settings) — test that env vars load correctly
3. Implement `logging.py` — verify JSON output to stdout
4. Implement `rate_limiter.py` — unit test the token bucket
5. Implement `api/client.py` — test with mocked httpx responses
6. Implement `db/connection.py` and `db/schema.py` — test against a real Neon DB (or local Postgres)
7. Implement `db/repositories.py` — test upserts with sample data
8. Implement `etl/extract.py`, `etl/transform.py`, `etl/load.py`
9. Implement `main.py` orchestrator
10. Implement `__main__.py` entry point
11. Create `Dockerfile`, `entrypoint.sh`, `docker-compose.yml`
12. Test end-to-end locally with `docker compose up`
13. Deploy to Portainer

### 13.2 Critical Implementation Details

- **Always use `ON CONFLICT` for all writes.** The ETL must be safe to re-run.
- **Never log secrets.** Mask `CLOUDTALK_API_KEY_SECRET` and `DATABASE_URL` in any debug output.
- **Rate limiter must be shared** across all API calls in a single run. Pass it to the client constructor.
- **Pagination must handle zero results.** If there are no calls for a day, the ETL should complete successfully with count=0.
- **Timestamps from CloudTalk use ISO 8601 format** and are in UTC. Store them as `TIMESTAMPTZ` in PostgreSQL.
- **The `type` field from call records is a reserved word in some contexts.** We store it as `call_type` in the database.
- **Phone numbers can be very long.** Use `TEXT` type, not `VARCHAR` with a limit.
- **The `Cdr.is_redirected` field returns "0" as a string** in the CloudTalk API. Handle this in the transform layer.
- **psycopg 3 uses `%()s` style parameters**, not `?` or `%s`. Use named parameters with dict-based parameterization.
- **Neon may have cold starts** — the first connection after inactivity can take a few seconds. The retry logic on the DB connection handles this.

### 13.3 Neon Database Setup

Before the first ETL run, create the Neon database:

1. Go to https://neon.tech and sign in
2. Create a new project (e.g., `bigbang-analytics`)
3. Select the region closest to the server (e.g., `eu-central-1` for a server in Slovenia)
4. Copy the connection string from the dashboard
5. Set it as the `DATABASE_URL` environment variable
6. Run the schema initialization: `python -m cloudtalk_etl.scripts.init_db`

### 13.4 Qlik Sense Connection

Once data is flowing, the Qlik team connects using:

1. In Qlik Sense: **Data load editor → Create new connection → PostgreSQL**
2. Host: `ep-example-name-12345.eu-central-1.aws.neon.tech`
3. Port: `5432`
4. Database: `cloudtalk_analytics`
5. Username/Password: From the Neon connection string
6. SSL: Required

They will then see the `calls`, `agents`, and `group_stats_daily` tables and can build their own Qlik data models and visualizations from there.
