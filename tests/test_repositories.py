"""
Unit tests for repository upsert functions.

Uses unittest.mock to avoid requiring a real database connection.
Verifies SQL generation, executemany batching, and commit behaviour.
"""
from unittest.mock import MagicMock

import pytest

from cloudtalk_etl.db.repositories import (
    upsert_call_center_groups,
    upsert_agent_stats,
    upsert_call_reasons,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_conn():
    """Return a mock psycopg Connection with a usable context-manager cursor."""
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    conn.cursor.return_value.__exit__.return_value = False
    return conn, cursor


def sample_cc_group() -> dict:
    return {
        "date": "03.03.2026",
        "country": "SLO",
        "group_name": "Reklamacije - SLO",
        "category": "Reklamacije",
        "total_calls": 142,
        "answered_calls": 118,
        "answered_pct": 83.10,
        "unanswered_calls": 24,
    }


def sample_agent_stat() -> dict:
    return {
        "date": "03.03.2026",
        "country": "SLO",
        "group_name": "Reklamacije - SLO",
        "category": "Reklamacije",
        "agent_id": 42,
        "agent_name": "Jane Doe",
        "presented_calls": 30,
        "answered_calls": 29,
        "talking_time_sec": 14400,
    }


def sample_call_reason() -> dict:
    return {
        "date": "03.03.2026",
        "country": "SLO",
        "group_name": "Reklamacije - SLO",
        "category": "Reklamacije",
        "tag_id": 5,
        "tag_name": "REKLAMACIJE",
        "call_count": 67,
    }


# ---------------------------------------------------------------------------
# upsert_call_center_groups
# ---------------------------------------------------------------------------

def test_upsert_call_center_groups_empty_list_returns_zero():
    conn, cursor = make_conn()
    result = upsert_call_center_groups(conn, [])
    assert result == 0
    cursor.executemany.assert_not_called()
    conn.commit.assert_not_called()


def test_upsert_call_center_groups_single_record():
    conn, cursor = make_conn()
    records = [sample_cc_group()]
    result = upsert_call_center_groups(conn, records)
    assert result == 1
    cursor.executemany.assert_called_once()
    sql, params = cursor.executemany.call_args.args
    assert "INSERT INTO call_center_groups" in sql
    assert "ON CONFLICT" in sql
    assert params == records
    conn.commit.assert_called_once()


def test_upsert_call_center_groups_sql_composite_pk():
    conn, cursor = make_conn()
    upsert_call_center_groups(conn, [sample_cc_group()])
    sql = cursor.executemany.call_args.args[0]
    assert "ON CONFLICT (date, country, group_name) DO UPDATE SET" in sql


def test_upsert_call_center_groups_sql_has_all_columns():
    conn, cursor = make_conn()
    upsert_call_center_groups(conn, [sample_cc_group()])
    sql = cursor.executemany.call_args.args[0]
    for col in ("date", "country", "group_name", "category",
                "total_calls", "answered_calls", "answered_pct", "unanswered_calls", "synced_at"):
        assert col in sql


def test_upsert_call_center_groups_multiple_records():
    conn, cursor = make_conn()
    records = [sample_cc_group(), {**sample_cc_group(), "group_name": "HR - Svetovanje", "country": "HR"}]
    result = upsert_call_center_groups(conn, records)
    assert result == 2
    conn.commit.assert_called_once()


# ---------------------------------------------------------------------------
# upsert_agent_stats
# ---------------------------------------------------------------------------

def test_upsert_agent_stats_empty_list_returns_zero():
    conn, cursor = make_conn()
    result = upsert_agent_stats(conn, [])
    assert result == 0
    cursor.executemany.assert_not_called()
    conn.commit.assert_not_called()


def test_upsert_agent_stats_single_record():
    conn, cursor = make_conn()
    records = [sample_agent_stat()]
    result = upsert_agent_stats(conn, records)
    assert result == 1
    cursor.executemany.assert_called_once()
    conn.commit.assert_called_once()


def test_upsert_agent_stats_sql_composite_pk():
    conn, cursor = make_conn()
    upsert_agent_stats(conn, [sample_agent_stat()])
    sql = cursor.executemany.call_args.args[0]
    assert "ON CONFLICT (date, country, group_name, agent_id) DO UPDATE SET" in sql


def test_upsert_agent_stats_sql_has_all_columns():
    conn, cursor = make_conn()
    upsert_agent_stats(conn, [sample_agent_stat()])
    sql = cursor.executemany.call_args.args[0]
    for col in ("date", "country", "group_name", "category",
                "agent_id", "agent_name", "presented_calls", "answered_calls",
                "talking_time_sec", "synced_at"):
        assert col in sql


def test_upsert_agent_stats_multiple_records():
    conn, cursor = make_conn()
    records = [sample_agent_stat(), {**sample_agent_stat(), "agent_id": 99, "agent_name": "Marko Horvat"}]
    result = upsert_agent_stats(conn, records)
    assert result == 2
    conn.commit.assert_called_once()


# ---------------------------------------------------------------------------
# upsert_call_reasons
# ---------------------------------------------------------------------------

def test_upsert_call_reasons_empty_list_returns_zero():
    conn, cursor = make_conn()
    result = upsert_call_reasons(conn, [])
    assert result == 0
    cursor.executemany.assert_not_called()
    conn.commit.assert_not_called()


def test_upsert_call_reasons_single_record():
    conn, cursor = make_conn()
    records = [sample_call_reason()]
    result = upsert_call_reasons(conn, records)
    assert result == 1
    cursor.executemany.assert_called_once()
    conn.commit.assert_called_once()


def test_upsert_call_reasons_sql_composite_pk():
    conn, cursor = make_conn()
    upsert_call_reasons(conn, [sample_call_reason()])
    sql = cursor.executemany.call_args.args[0]
    assert "ON CONFLICT (date, country, group_name, tag_id) DO UPDATE SET" in sql


def test_upsert_call_reasons_sql_has_all_columns():
    conn, cursor = make_conn()
    upsert_call_reasons(conn, [sample_call_reason()])
    sql = cursor.executemany.call_args.args[0]
    for col in ("date", "country", "group_name", "category",
                "tag_id", "tag_name", "call_count", "synced_at"):
        assert col in sql


def test_upsert_call_reasons_multiple_records():
    conn, cursor = make_conn()
    records = [sample_call_reason(), {**sample_call_reason(), "tag_id": 13, "tag_name": "SVETOVANJE PRI PRODAJI"}]
    result = upsert_call_reasons(conn, records)
    assert result == 2
    conn.commit.assert_called_once()
