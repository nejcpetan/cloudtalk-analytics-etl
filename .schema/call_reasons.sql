-- TABLE: call_reasons
-- Daily per-group call reason (tag) counts.
-- Granularity: one row per (date, country, group_name, tag_id).
-- Tags are applied by agents post-call to classify the reason for contact.
-- Updated nightly via upsert (ON CONFLICT DO UPDATE).

CREATE TABLE IF NOT EXISTS call_reasons (
    date             TEXT         NOT NULL,   -- format: DD.MM.YYYY  e.g. "09.03.2026"
    country          TEXT         NOT NULL,   -- "SLO" or "CRO"
    group_name       TEXT         NOT NULL,   -- full queue name  e.g. "Reklamacije - SLO"
    category         TEXT         NOT NULL,   -- parsed name part e.g. "Reklamacije"
    tag_id           INTEGER      NOT NULL,   -- CloudTalk internal tag ID
    tag_name         TEXT,                    -- tag label text, NULL if unavailable
    call_count       INTEGER      DEFAULT 0,  -- number of calls with this tag in this group
    synced_at        TIMESTAMPTZ  DEFAULT NOW(),

    PRIMARY KEY (date, country, group_name, tag_id)
);
