import psycopg
import structlog

logger = structlog.get_logger()

_SCHEMA_SQL = """
-- Calls table: one row per call
CREATE TABLE IF NOT EXISTS calls (
    id                  BIGINT PRIMARY KEY,
    call_type           TEXT NOT NULL,
    billsec             INTEGER DEFAULT 0,
    talking_time        INTEGER DEFAULT 0,
    waiting_time        INTEGER DEFAULT 0,
    wrapup_time         INTEGER DEFAULT 0,
    public_external     TEXT,
    public_internal     TEXT,
    country_code        TEXT,
    recorded            BOOLEAN DEFAULT FALSE,
    is_voicemail        BOOLEAN DEFAULT FALSE,
    is_redirected       BOOLEAN DEFAULT FALSE,
    redirected_from     TEXT,
    user_id             TEXT,
    started_at          TIMESTAMPTZ,
    answered_at         TIMESTAMPTZ,
    ended_at            TIMESTAMPTZ,
    recording_link      TEXT,
    call_status         TEXT,
    call_date           DATE,
    contact_id          TEXT,
    contact_name        TEXT,
    contact_company     TEXT,
    agent_id            TEXT,
    agent_name          TEXT,
    synced_at           TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_calls_call_date ON calls (call_date);
CREATE INDEX IF NOT EXISTS idx_calls_user_id ON calls (user_id);
CREATE INDEX IF NOT EXISTS idx_calls_call_type ON calls (call_type);
CREATE INDEX IF NOT EXISTS idx_calls_call_status ON calls (call_status);
CREATE INDEX IF NOT EXISTS idx_calls_started_at ON calls (started_at);

-- Migrations: add columns that may not exist in older deployments
ALTER TABLE calls ADD COLUMN IF NOT EXISTS agent_id   TEXT;
ALTER TABLE calls ADD COLUMN IF NOT EXISTS agent_name TEXT;

-- Agents table: one row per agent per sync date
CREATE TABLE IF NOT EXISTS agents (
    id                  TEXT NOT NULL,
    sync_date           DATE NOT NULL,
    firstname           TEXT,
    lastname            TEXT,
    fullname            TEXT,
    email               TEXT,
    availability_status TEXT,
    extension           TEXT,
    default_number      TEXT,
    associated_numbers  TEXT[],
    synced_at           TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (id, sync_date)
);

CREATE INDEX IF NOT EXISTS idx_agents_sync_date ON agents (sync_date);

-- Group statistics: one row per group per sync date
CREATE TABLE IF NOT EXISTS group_stats_daily (
    group_id                INTEGER NOT NULL,
    group_name              TEXT NOT NULL,
    sync_date               DATE NOT NULL,
    operators               INTEGER DEFAULT 0,
    answered                INTEGER DEFAULT 0,
    unanswered              INTEGER DEFAULT 0,
    abandon_rate            REAL DEFAULT 0.0,
    avg_waiting_time        INTEGER DEFAULT 0,
    max_waiting_time        INTEGER DEFAULT 0,
    avg_call_duration       INTEGER DEFAULT 0,
    rt_waiting_queue        INTEGER DEFAULT 0,
    rt_avg_waiting_time     INTEGER DEFAULT 0,
    rt_max_waiting_time     INTEGER DEFAULT 0,
    rt_avg_abandonment_time INTEGER DEFAULT 0,
    synced_at               TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (group_id, sync_date)
);

CREATE INDEX IF NOT EXISTS idx_group_stats_sync_date ON group_stats_daily (sync_date);

-- Phase 2: Conversation Intelligence (created but not populated until enabled)
CREATE TABLE IF NOT EXISTS call_intelligence (
    call_id             BIGINT PRIMARY KEY REFERENCES calls(id),
    summary             TEXT,
    overall_sentiment   JSONB,
    talk_listen_ratio   JSONB,
    topics              JSONB,
    transcription       JSONB,
    smart_notes         TEXT,
    synced_at           TIMESTAMPTZ DEFAULT NOW()
);
"""


def ensure_schema(conn: psycopg.Connection) -> None:
    """Create all tables and indexes if they don't exist. Safe to re-run."""
    with conn.cursor() as cur:
        cur.execute(_SCHEMA_SQL)
    conn.commit()
    logger.info("schema_ensured")
