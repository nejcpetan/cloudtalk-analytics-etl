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
