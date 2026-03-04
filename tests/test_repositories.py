"""
Unit tests for repository upsert functions.

Uses unittest.mock to avoid requiring a real database connection.
Verifies SQL generation, executemany batching, and commit behaviour.
"""
from datetime import date
from unittest.mock import MagicMock, call, patch

import pytest

from cloudtalk_etl.db.repositories import upsert_calls, upsert_agents, upsert_group_stats

SYNC_DATE = date(2026, 3, 3)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_conn():
    """Return a mock psycopg Connection with a usable context-manager cursor."""
    conn = MagicMock()
    cursor = MagicMock()
    # conn.cursor() used as context manager: __enter__ returns cursor
    conn.cursor.return_value.__enter__.return_value = cursor
    conn.cursor.return_value.__exit__.return_value = False
    return conn, cursor


def sample_call() -> dict:
    return {
        "id": 100001,
        "call_type": "incoming",
        "billsec": 120,
        "talking_time": 115,
        "waiting_time": 5,
        "wrapup_time": 30,
        "public_external": "+38641000001",
        "public_internal": "101",
        "country_code": "SI",
        "recorded": True,
        "is_voicemail": False,
        "is_redirected": False,
        "redirected_from": None,
        "user_id": "42",
        "started_at": "2026-03-03T09:00:00Z",
        "answered_at": "2026-03-03T09:00:05Z",
        "ended_at": "2026-03-03T09:02:00Z",
        "recording_link": "https://example.com/rec.mp3",
        "call_status": "answered",
        "call_date": date(2026, 3, 3),
        "contact_id": "999",
        "contact_name": "Test Customer",
        "contact_company": "ACME d.o.o.",
    }


def sample_agent() -> dict:
    return {
        "id": "42",
        "sync_date": SYNC_DATE,
        "firstname": "Jana",
        "lastname": "Novak",
        "fullname": "Jana Novak",
        "email": "jana@example.com",
        "availability_status": "online",
        "extension": "101",
        "default_number": "+38641000001",
        "associated_numbers": ["+38641000001"],
    }


def sample_group_stat() -> dict:
    return {
        "group_id": 10,
        "group_name": "Support",
        "sync_date": SYNC_DATE,
        "operators": 5,
        "answered": 50,
        "unanswered": 10,
        "abandon_rate": 16.67,
        "avg_waiting_time": 30,
        "max_waiting_time": 120,
        "avg_call_duration": 180,
        "rt_waiting_queue": 3,
        "rt_avg_waiting_time": 25,
        "rt_max_waiting_time": 90,
        "rt_avg_abandonment_time": 15,
    }


# ---------------------------------------------------------------------------
# upsert_calls
# ---------------------------------------------------------------------------

def test_upsert_calls_empty_list_returns_zero():
    conn, cursor = make_conn()
    result = upsert_calls(conn, [])
    assert result == 0
    cursor.executemany.assert_not_called()
    conn.commit.assert_not_called()


def test_upsert_calls_single_record():
    conn, cursor = make_conn()
    records = [sample_call()]
    result = upsert_calls(conn, records)
    assert result == 1
    cursor.executemany.assert_called_once()
    # First arg is the SQL string, second is the list of records
    sql, params = cursor.executemany.call_args.args
    assert "INSERT INTO calls" in sql
    assert "ON CONFLICT" in sql
    assert params == records
    conn.commit.assert_called_once()


def test_upsert_calls_multiple_records():
    conn, cursor = make_conn()
    records = [sample_call(), {**sample_call(), "id": 100002}]
    result = upsert_calls(conn, records)
    assert result == 2
    conn.commit.assert_called_once()


def test_upsert_calls_sql_contains_on_conflict():
    conn, cursor = make_conn()
    upsert_calls(conn, [sample_call()])
    sql = cursor.executemany.call_args.args[0]
    assert "ON CONFLICT (id) DO UPDATE SET" in sql


def test_upsert_calls_sql_has_all_key_columns():
    conn, cursor = make_conn()
    upsert_calls(conn, [sample_call()])
    sql = cursor.executemany.call_args.args[0]
    for col in ("id", "call_type", "billsec", "call_status", "call_date",
                "contact_name", "synced_at"):
        assert col in sql


# ---------------------------------------------------------------------------
# upsert_agents
# ---------------------------------------------------------------------------

def test_upsert_agents_empty_list_returns_zero():
    conn, cursor = make_conn()
    result = upsert_agents(conn, [], SYNC_DATE)
    assert result == 0
    cursor.executemany.assert_not_called()


def test_upsert_agents_single_record():
    conn, cursor = make_conn()
    records = [sample_agent()]
    result = upsert_agents(conn, records, SYNC_DATE)
    assert result == 1
    cursor.executemany.assert_called_once()
    conn.commit.assert_called_once()


def test_upsert_agents_sql_composite_pk():
    conn, cursor = make_conn()
    upsert_agents(conn, [sample_agent()], SYNC_DATE)
    sql = cursor.executemany.call_args.args[0]
    assert "ON CONFLICT (id, sync_date) DO UPDATE SET" in sql


def test_upsert_agents_sql_has_associated_numbers():
    conn, cursor = make_conn()
    upsert_agents(conn, [sample_agent()], SYNC_DATE)
    sql = cursor.executemany.call_args.args[0]
    assert "associated_numbers" in sql


def test_upsert_agents_multiple_records():
    conn, cursor = make_conn()
    records = [sample_agent(), {**sample_agent(), "id": "99"}]
    result = upsert_agents(conn, records, SYNC_DATE)
    assert result == 2


# ---------------------------------------------------------------------------
# upsert_group_stats
# ---------------------------------------------------------------------------

def test_upsert_group_stats_empty_list_returns_zero():
    conn, cursor = make_conn()
    result = upsert_group_stats(conn, [], SYNC_DATE)
    assert result == 0
    cursor.executemany.assert_not_called()


def test_upsert_group_stats_single_record():
    conn, cursor = make_conn()
    records = [sample_group_stat()]
    result = upsert_group_stats(conn, records, SYNC_DATE)
    assert result == 1
    cursor.executemany.assert_called_once()
    conn.commit.assert_called_once()


def test_upsert_group_stats_sql_composite_pk():
    conn, cursor = make_conn()
    upsert_group_stats(conn, [sample_group_stat()], SYNC_DATE)
    sql = cursor.executemany.call_args.args[0]
    assert "ON CONFLICT (group_id, sync_date) DO UPDATE SET" in sql


def test_upsert_group_stats_sql_has_realtime_fields():
    conn, cursor = make_conn()
    upsert_group_stats(conn, [sample_group_stat()], SYNC_DATE)
    sql = cursor.executemany.call_args.args[0]
    assert "rt_waiting_queue" in sql
    assert "rt_avg_abandonment_time" in sql


def test_upsert_group_stats_multiple_records():
    conn, cursor = make_conn()
    records = [sample_group_stat(), {**sample_group_stat(), "group_id": 11}]
    result = upsert_group_stats(conn, records, SYNC_DATE)
    assert result == 2
