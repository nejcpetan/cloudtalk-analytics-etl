import psycopg
import structlog

from cloudtalk_etl.db.repositories import (
    upsert_call_center_groups,
    upsert_agent_stats,
    upsert_call_reasons,
)

logger = structlog.get_logger()

BATCH_SIZE = 500


def load_call_center_groups(conn: psycopg.Connection, rows: list[dict]) -> int:
    """Load call_center_groups rows in batches."""
    total = 0
    for i in range(0, max(len(rows), 1), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        if batch:
            total += upsert_call_center_groups(conn, batch)
    logger.info("loaded_call_center_groups", total=total)
    return total


def load_agent_stats(conn: psycopg.Connection, rows: list[dict]) -> int:
    """Load agent_stats rows in batches."""
    total = 0
    for i in range(0, max(len(rows), 1), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        if batch:
            total += upsert_agent_stats(conn, batch)
    logger.info("loaded_agent_stats", total=total)
    return total


def load_call_reasons(conn: psycopg.Connection, rows: list[dict]) -> int:
    """Load call_reasons rows in batches."""
    total = 0
    for i in range(0, max(len(rows), 1), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        if batch:
            total += upsert_call_reasons(conn, batch)
    logger.info("loaded_call_reasons", total=total)
    return total
