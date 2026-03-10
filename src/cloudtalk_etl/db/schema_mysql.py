import structlog

logger = structlog.get_logger()

# MySQL 8.0+ compatible DDL.
# Executed as separate statements because MySQL does not support
# multiple DDL statements in a single execute() call.
_SCHEMA_STATEMENTS = [
    # --- call_center_groups ---
    """
    CREATE TABLE IF NOT EXISTS call_center_groups (
        date             VARCHAR(10)   NOT NULL,
        country          VARCHAR(10)   NOT NULL,
        group_name       VARCHAR(255)  NOT NULL,
        category         VARCHAR(255)  NOT NULL,
        total_calls      INT           DEFAULT 0,
        answered_calls   INT           DEFAULT 0,
        answered_pct     DECIMAL(5,2),
        unanswered_calls INT           DEFAULT 0,
        synced_at        DATETIME      DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (date, country, group_name)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    "CREATE INDEX IF NOT EXISTS idx_call_center_groups_date    ON call_center_groups (date)",
    "CREATE INDEX IF NOT EXISTS idx_call_center_groups_country ON call_center_groups (country)",

    # --- agent_stats ---
    """
    CREATE TABLE IF NOT EXISTS agent_stats (
        date             VARCHAR(10)   NOT NULL,
        country          VARCHAR(10)   NOT NULL,
        group_name       VARCHAR(255)  NOT NULL,
        category         VARCHAR(255)  NOT NULL,
        agent_id         INT           NOT NULL,
        agent_name       VARCHAR(255),
        presented_calls  INT           DEFAULT 0,
        answered_calls   INT           DEFAULT 0,
        talking_time_sec INT           DEFAULT 0,
        synced_at        DATETIME      DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (date, country, group_name, agent_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    "CREATE INDEX IF NOT EXISTS idx_agent_stats_date ON agent_stats (date)",

    # --- call_reasons ---
    """
    CREATE TABLE IF NOT EXISTS call_reasons (
        date             VARCHAR(10)   NOT NULL,
        country          VARCHAR(10)   NOT NULL,
        group_name       VARCHAR(255)  NOT NULL,
        category         VARCHAR(255)  NOT NULL,
        tag_id           INT           NOT NULL,
        tag_name         VARCHAR(255),
        call_count       INT           DEFAULT 0,
        synced_at        DATETIME      DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (date, country, group_name, tag_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    "CREATE INDEX IF NOT EXISTS idx_call_reasons_date ON call_reasons (date)",
]


def ensure_schema(conn) -> None:
    """
    Create the 3 output tables if they don't already exist.

    Safe to re-run on every ETL execution: CREATE TABLE IF NOT EXISTS is idempotent.
    """
    cursor = conn.cursor()
    try:
        for sql in _SCHEMA_STATEMENTS:
            cursor.execute(sql)
        conn.commit()
    finally:
        cursor.close()
    logger.info("schema_ensured", backend="mysql")
