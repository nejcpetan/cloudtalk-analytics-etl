import psycopg
import structlog

logger = structlog.get_logger()

_SCHEMA_SQL = """
-- call_center_groups: daily aggregated call stats per group (Table 1 / Block A)
CREATE TABLE IF NOT EXISTS call_center_groups (
    date             TEXT         NOT NULL,   -- DD.MM.YYYY
    country          TEXT         NOT NULL,   -- SLO | HR | UNKNOWN
    group_name       TEXT         NOT NULL,   -- full original group name
    category         TEXT         NOT NULL,   -- parsed from group name
    total_calls      INTEGER      DEFAULT 0,
    answered_calls   INTEGER      DEFAULT 0,
    answered_pct     NUMERIC(5,2),            -- NULL if total_calls = 0
    unanswered_calls INTEGER      DEFAULT 0,
    synced_at        TIMESTAMPTZ  DEFAULT NOW(),
    PRIMARY KEY (date, country, group_name)
);

CREATE INDEX IF NOT EXISTS idx_call_center_groups_date    ON call_center_groups (date);
CREATE INDEX IF NOT EXISTS idx_call_center_groups_country ON call_center_groups (country);

-- agent_stats: daily per-agent call stats per group (Table 2 / Block B)
CREATE TABLE IF NOT EXISTS agent_stats (
    date             TEXT         NOT NULL,   -- DD.MM.YYYY
    country          TEXT         NOT NULL,
    group_name       TEXT         NOT NULL,
    category         TEXT         NOT NULL,
    agent_id         INTEGER      NOT NULL,
    agent_name       TEXT,
    presented_calls  INTEGER      DEFAULT 0,
    answered_calls   INTEGER      DEFAULT 0,
    talking_time_sec INTEGER      DEFAULT 0,
    synced_at        TIMESTAMPTZ  DEFAULT NOW(),
    PRIMARY KEY (date, country, group_name, agent_id)
);

CREATE INDEX IF NOT EXISTS idx_agent_stats_date ON agent_stats (date);

-- call_reasons: daily tag usage counts per group (Table 3 / Block C)
CREATE TABLE IF NOT EXISTS call_reasons (
    date             TEXT         NOT NULL,   -- DD.MM.YYYY
    country          TEXT         NOT NULL,
    group_name       TEXT         NOT NULL,
    category         TEXT         NOT NULL,
    tag_id           INTEGER      NOT NULL,
    tag_name         TEXT,
    call_count       INTEGER      DEFAULT 0,
    synced_at        TIMESTAMPTZ  DEFAULT NOW(),
    PRIMARY KEY (date, country, group_name, tag_id)
);

CREATE INDEX IF NOT EXISTS idx_call_reasons_date ON call_reasons (date);
"""


def ensure_schema(conn: psycopg.Connection) -> None:
    """
    Create the 3 output tables if they don't already exist.

    Safe to re-run on every ETL execution: CREATE TABLE IF NOT EXISTS is idempotent.
    """
    with conn.cursor() as cur:
        cur.execute(_SCHEMA_SQL)
    conn.commit()
    logger.info("schema_ensured")
