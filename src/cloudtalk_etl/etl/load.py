import psycopg
import structlog
from datetime import date

from cloudtalk_etl.db.repositories import (
    upsert_calls,
    upsert_agents,
    upsert_group_stats,
    upsert_numbers_dim,
    upsert_groups_dim,
    upsert_tags_dim,
    upsert_call_tags,
    upsert_call_center_daily_stats,
    upsert_agent_daily_stats,
    upsert_call_reasons_daily,
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


# ===========================================================================
# Phase 2: Dimension loaders
# ===========================================================================

def load_numbers_dim(conn: psycopg.Connection, numbers: list[dict]) -> int:
    """Load number dimension records."""
    return upsert_numbers_dim(conn, numbers)


def load_groups_dim(conn: psycopg.Connection, groups: list[dict]) -> int:
    """Load group dimension records."""
    return upsert_groups_dim(conn, groups)


def load_tags_dim(conn: psycopg.Connection, tags: list[dict]) -> int:
    """Load tag dimension records."""
    return upsert_tags_dim(conn, tags)


# ===========================================================================
# Phase 2: Fact / bridge loaders
# ===========================================================================

def load_call_tags(conn: psycopg.Connection, call_tags: list[dict]) -> int:
    """Load call-tag bridge records in batches."""
    total = 0
    for i in range(0, len(call_tags), BATCH_SIZE):
        batch = call_tags[i:i + BATCH_SIZE]
        total += upsert_call_tags(conn, batch)
    return total


def load_call_center_daily_stats(conn: psycopg.Connection, stats: list[dict],
                                  sync_date: date) -> int:
    """Load call center daily aggregated statistics."""
    return upsert_call_center_daily_stats(conn, stats, sync_date)


def load_agent_daily_stats(conn: psycopg.Connection, stats: list[dict],
                            sync_date: date) -> int:
    """Load agent daily aggregated statistics."""
    return upsert_agent_daily_stats(conn, stats, sync_date)


def load_call_reasons_daily(conn: psycopg.Connection, reasons: list[dict],
                             sync_date: date) -> int:
    """Load call reason tag counts per group per day."""
    return upsert_call_reasons_daily(conn, reasons, sync_date)
