-- TABLE: call_center_groups
-- Daily aggregated call statistics per call center group.
-- Granularity: one row per (date, country, group_name).
-- Updated nightly via upsert (ON CONFLICT DO UPDATE).

CREATE TABLE IF NOT EXISTS call_center_groups (
    date             TEXT         NOT NULL,   -- format: DD.MM.YYYY  e.g. "09.03.2026"
    country          TEXT         NOT NULL,   -- "SLO" or "CRO"
    group_name       TEXT         NOT NULL,   -- full queue name  e.g. "Reklamacije - SLO"
    category         TEXT         NOT NULL,   -- parsed name part e.g. "Reklamacije"
    total_calls      INTEGER      DEFAULT 0,  -- all inbound calls routed to this group
    answered_calls   INTEGER      DEFAULT 0,  -- calls answered by an agent
    answered_pct     NUMERIC(5,2),            -- answered / total * 100, NULL if total = 0
    unanswered_calls INTEGER      DEFAULT 0,  -- missed / abandoned calls
    synced_at        TIMESTAMPTZ  DEFAULT NOW(),

    PRIMARY KEY (date, country, group_name)
);

-- Known groups (6 business queues):
--   SLO: "Reklamacije - SLO", "Svetovanje Pri Prodaji - SLO", "Stanje Narocil (Splet) - SLO"
--   CRO: "Opce Informacije - CRO", "Status Narudzbe - CRO", "Reklamacije i Servis - CRO"
