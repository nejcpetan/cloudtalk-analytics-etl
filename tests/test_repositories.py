"""
Unit tests for repository upsert functions.

Uses unittest.mock to avoid requiring a real database connection.
Verifies SQL generation, executemany batching, and commit behaviour.
"""
from datetime import date
from unittest.mock import MagicMock, call, patch

import pytest

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


# ===========================================================================
# Phase 2: Dimension upserts
# ===========================================================================

def sample_number() -> dict:
    return {
        "id": 7,
        "internal_name": "Reklamacije SLO",
        "caller_id_e164": "+38612345678",
        "country_code": 386,
        "connected_to": 0,
        "source_id": 10,
    }


def sample_group_dim() -> dict:
    return {"id": 10, "internal_name": "Reklamacije SLO"}


def sample_tag() -> dict:
    return {"id": 5, "name": "REKLAMACIJE"}


def sample_call_tag() -> dict:
    return {"call_id": 100001, "tag_id": 5, "tag_name": "REKLAMACIJE"}


def sample_cc_stat() -> dict:
    return {
        "sync_date": SYNC_DATE,
        "group_id": 10,
        "group_name": "Reklamacije SLO",
        "country_code": 386,
        "total_calls": 142,
        "answered_calls": 118,
        "missed_calls": 24,
        "callback_calls": 0,
        "answer_rate_pct": 83.10,
    }


def sample_agent_stat() -> dict:
    return {
        "sync_date": SYNC_DATE,
        "agent_id": 42,
        "agent_name": "Jane Doe",
        "presented_calls": 0,
        "answered_calls": 29,
        "total_talk_seconds": 14400,
    }


def sample_reason() -> dict:
    return {
        "sync_date": SYNC_DATE,
        "group_id": 10,
        "group_name": "Reklamacije SLO",
        "tag_id": 5,
        "tag_name": "REKLAMACIJE",
        "call_count": 67,
    }


# ---------------------------------------------------------------------------
# upsert_numbers_dim
# ---------------------------------------------------------------------------

def test_upsert_numbers_dim_empty_returns_zero():
    conn, cursor = make_conn()
    assert upsert_numbers_dim(conn, []) == 0
    cursor.executemany.assert_not_called()


def test_upsert_numbers_dim_single_record():
    conn, cursor = make_conn()
    result = upsert_numbers_dim(conn, [sample_number()])
    assert result == 1
    cursor.executemany.assert_called_once()
    conn.commit.assert_called_once()


def test_upsert_numbers_dim_sql_on_conflict():
    conn, cursor = make_conn()
    upsert_numbers_dim(conn, [sample_number()])
    sql = cursor.executemany.call_args.args[0]
    assert "ON CONFLICT (id) DO UPDATE SET" in sql
    assert "caller_id_e164" in sql
    assert "source_id" in sql


# ---------------------------------------------------------------------------
# upsert_groups_dim
# ---------------------------------------------------------------------------

def test_upsert_groups_dim_empty_returns_zero():
    conn, cursor = make_conn()
    assert upsert_groups_dim(conn, []) == 0


def test_upsert_groups_dim_single_record():
    conn, cursor = make_conn()
    result = upsert_groups_dim(conn, [sample_group_dim()])
    assert result == 1
    sql = cursor.executemany.call_args.args[0]
    assert "ON CONFLICT (id) DO UPDATE SET" in sql
    assert "internal_name" in sql


# ---------------------------------------------------------------------------
# upsert_tags_dim
# ---------------------------------------------------------------------------

def test_upsert_tags_dim_empty_returns_zero():
    conn, cursor = make_conn()
    assert upsert_tags_dim(conn, []) == 0


def test_upsert_tags_dim_single_record():
    conn, cursor = make_conn()
    result = upsert_tags_dim(conn, [sample_tag()])
    assert result == 1
    sql = cursor.executemany.call_args.args[0]
    assert "ON CONFLICT (id) DO UPDATE SET" in sql
    assert "name" in sql


# ---------------------------------------------------------------------------
# upsert_call_tags
# ---------------------------------------------------------------------------

def test_upsert_call_tags_empty_returns_zero():
    conn, cursor = make_conn()
    assert upsert_call_tags(conn, []) == 0


def test_upsert_call_tags_single_record():
    conn, cursor = make_conn()
    result = upsert_call_tags(conn, [sample_call_tag()])
    assert result == 1
    conn.commit.assert_called_once()


def test_upsert_call_tags_sql_do_nothing():
    conn, cursor = make_conn()
    upsert_call_tags(conn, [sample_call_tag()])
    sql = cursor.executemany.call_args.args[0]
    assert "ON CONFLICT (call_id, tag_id) DO NOTHING" in sql


def test_upsert_call_tags_multiple_records():
    conn, cursor = make_conn()
    records = [sample_call_tag(), {**sample_call_tag(), "tag_id": 13, "tag_name": "SVETOVANJE PRI PRODAJI"}]
    result = upsert_call_tags(conn, records)
    assert result == 2


# ---------------------------------------------------------------------------
# upsert_call_center_daily_stats
# ---------------------------------------------------------------------------

def test_upsert_call_center_daily_stats_empty_returns_zero():
    conn, cursor = make_conn()
    assert upsert_call_center_daily_stats(conn, [], SYNC_DATE) == 0


def test_upsert_call_center_daily_stats_single_record():
    conn, cursor = make_conn()
    result = upsert_call_center_daily_stats(conn, [sample_cc_stat()], SYNC_DATE)
    assert result == 1
    conn.commit.assert_called_once()


def test_upsert_call_center_daily_stats_sql_composite_pk():
    conn, cursor = make_conn()
    upsert_call_center_daily_stats(conn, [sample_cc_stat()], SYNC_DATE)
    sql = cursor.executemany.call_args.args[0]
    assert "ON CONFLICT (sync_date, group_id) DO UPDATE SET" in sql
    assert "answer_rate_pct" in sql
    assert "country_code" in sql


# ---------------------------------------------------------------------------
# upsert_agent_daily_stats
# ---------------------------------------------------------------------------

def test_upsert_agent_daily_stats_empty_returns_zero():
    conn, cursor = make_conn()
    assert upsert_agent_daily_stats(conn, [], SYNC_DATE) == 0


def test_upsert_agent_daily_stats_single_record():
    conn, cursor = make_conn()
    result = upsert_agent_daily_stats(conn, [sample_agent_stat()], SYNC_DATE)
    assert result == 1
    conn.commit.assert_called_once()


def test_upsert_agent_daily_stats_sql_composite_pk():
    conn, cursor = make_conn()
    upsert_agent_daily_stats(conn, [sample_agent_stat()], SYNC_DATE)
    sql = cursor.executemany.call_args.args[0]
    assert "ON CONFLICT (sync_date, agent_id) DO UPDATE SET" in sql
    assert "total_talk_seconds" in sql
    assert "presented_calls" in sql


# ---------------------------------------------------------------------------
# upsert_call_reasons_daily
# ---------------------------------------------------------------------------

def test_upsert_call_reasons_daily_empty_returns_zero():
    conn, cursor = make_conn()
    assert upsert_call_reasons_daily(conn, [], SYNC_DATE) == 0


def test_upsert_call_reasons_daily_single_record():
    conn, cursor = make_conn()
    result = upsert_call_reasons_daily(conn, [sample_reason()], SYNC_DATE)
    assert result == 1
    conn.commit.assert_called_once()


def test_upsert_call_reasons_daily_sql_composite_pk():
    conn, cursor = make_conn()
    upsert_call_reasons_daily(conn, [sample_reason()], SYNC_DATE)
    sql = cursor.executemany.call_args.args[0]
    assert "ON CONFLICT (sync_date, group_id, tag_id) DO UPDATE SET" in sql
    assert "call_count" in sql
    assert "tag_name" in sql
