-- TABLE: agent_stats
-- Daily per-agent call statistics, broken down by group.
-- Granularity: one row per (date, country, group_name, agent_id).
-- An agent appears in multiple groups if they handle multiple queues.
-- Updated nightly via upsert (ON CONFLICT DO UPDATE).

CREATE TABLE IF NOT EXISTS agent_stats (
    date             TEXT         NOT NULL,   -- format: DD.MM.YYYY  e.g. "09.03.2026"
    country          TEXT         NOT NULL,   -- "SLO" or "CRO"
    group_name       TEXT         NOT NULL,   -- full queue name  e.g. "Reklamacije - SLO"
    category         TEXT         NOT NULL,   -- parsed name part e.g. "Reklamacije"
    agent_id         INTEGER      NOT NULL,   -- CloudTalk internal agent ID
    agent_name       TEXT,                    -- agent full name, NULL if unavailable
    presented_calls  INTEGER      DEFAULT 0,  -- number of times the agent's phone rang
    answered_calls   INTEGER      DEFAULT 0,  -- number of calls the agent picked up
    talking_time_sec INTEGER      DEFAULT 0,  -- total seconds spent talking (answered calls only)
    synced_at        TIMESTAMPTZ  DEFAULT NOW(),

    PRIMARY KEY (date, country, group_name, agent_id)
);
