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

-- Phase 2: Dimension tables

-- numbers_dim: call center phone numbers with routing info (number → group mapping)
CREATE TABLE IF NOT EXISTS numbers_dim (
    id               INTEGER PRIMARY KEY,
    internal_name    TEXT,
    caller_id_e164   TEXT,
    country_code     INTEGER,        -- 386=SLO, 385=HR
    connected_to     INTEGER,        -- 0=group, 1=agent, 2=conference, 3=fax
    source_id        INTEGER,        -- group_id when connected_to=0
    synced_at        TIMESTAMPTZ DEFAULT NOW()
);

-- groups_dim: call center queue groups (IVR destination groups / "ponorna številke")
CREATE TABLE IF NOT EXISTS groups_dim (
    id            INTEGER PRIMARY KEY,
    internal_name TEXT NOT NULL,
    synced_at     TIMESTAMPTZ DEFAULT NOW()
);

-- tags_dim: call reason tags applied by agents post-call
CREATE TABLE IF NOT EXISTS tags_dim (
    id        INTEGER PRIMARY KEY,
    name      TEXT NOT NULL,
    synced_at TIMESTAMPTZ DEFAULT NOW()
);

-- call_tags: which tags were applied to each call (many-to-many bridge)
CREATE TABLE IF NOT EXISTS call_tags (
    call_id  BIGINT  NOT NULL,
    tag_id   INTEGER NOT NULL,
    tag_name TEXT,
    PRIMARY KEY (call_id, tag_id)
);

-- call_center_daily_stats: aggregated call stats per group per day (Block A reporting)
CREATE TABLE IF NOT EXISTS call_center_daily_stats (
    sync_date       DATE    NOT NULL,
    group_id        INTEGER NOT NULL,
    group_name      TEXT    NOT NULL,
    country_code    INTEGER,
    total_calls     INTEGER DEFAULT 0,
    answered_calls  INTEGER DEFAULT 0,
    missed_calls    INTEGER DEFAULT 0,
    callback_calls  INTEGER DEFAULT 0,
    answer_rate_pct NUMERIC(5,2),
    synced_at       TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (sync_date, group_id)
);

CREATE INDEX IF NOT EXISTS idx_call_center_daily_sync_date ON call_center_daily_stats (sync_date);
CREATE INDEX IF NOT EXISTS idx_call_center_daily_country   ON call_center_daily_stats (country_code);

-- agent_daily_stats: aggregated call stats per agent per day (Block B reporting)
CREATE TABLE IF NOT EXISTS agent_daily_stats (
    sync_date          DATE    NOT NULL,
    agent_id           INTEGER NOT NULL,
    agent_name         TEXT,
    presented_calls    INTEGER DEFAULT 0,
    answered_calls     INTEGER DEFAULT 0,
    total_talk_seconds INTEGER DEFAULT 0,
    synced_at          TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (sync_date, agent_id)
);

CREATE INDEX IF NOT EXISTS idx_agent_daily_stats_sync_date ON agent_daily_stats (sync_date);

-- call_reasons_daily: tag usage counts per group per day (Block C reporting)
CREATE TABLE IF NOT EXISTS call_reasons_daily (
    sync_date   DATE    NOT NULL,
    group_id    INTEGER NOT NULL,
    group_name  TEXT,
    tag_id      INTEGER NOT NULL,
    tag_name    TEXT,
    call_count  INTEGER DEFAULT 0,
    synced_at   TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (sync_date, group_id, tag_id)
);

CREATE INDEX IF NOT EXISTS idx_call_reasons_daily_sync_date ON call_reasons_daily (sync_date);
"""


def ensure_schema(conn: psycopg.Connection) -> None:
    """Create all tables and indexes if they don't exist. Safe to re-run."""
    with conn.cursor() as cur:
        cur.execute(_SCHEMA_SQL)
    conn.commit()
    logger.info("schema_ensured")
