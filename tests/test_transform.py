"""Unit tests for ETL transform functions."""
import pytest
from datetime import date

from cloudtalk_etl.etl.transform import (
    transform_calls,
    transform_agents,
    transform_group_stats,
    safe_int,
    safe_float,
    parse_timestamp,
)

SYNC_DATE = date(2026, 3, 3)


# --- safe_int ---

def test_safe_int_valid_string():
    assert safe_int("42") == 42


def test_safe_int_valid_int():
    assert safe_int(42) == 42


def test_safe_int_none_returns_default():
    assert safe_int(None) == 0


def test_safe_int_garbage_returns_default():
    assert safe_int("abc") == 0


def test_safe_int_custom_default():
    assert safe_int(None, default=99) == 99


# --- safe_float ---

def test_safe_float_valid_string():
    assert safe_float("3.14") == pytest.approx(3.14)


def test_safe_float_none_returns_default():
    assert safe_float(None) == 0.0


def test_safe_float_garbage_returns_default():
    assert safe_float("bad") == 0.0


# --- parse_timestamp ---

def test_parse_timestamp_valid_iso():
    result = parse_timestamp("2026-03-03T09:00:00Z")
    assert result == "2026-03-03T09:00:00Z"


def test_parse_timestamp_none_returns_none():
    assert parse_timestamp(None) is None


def test_parse_timestamp_empty_string_returns_none():
    assert parse_timestamp("") is None


def test_parse_timestamp_zero_returns_none():
    assert parse_timestamp("0") is None


def test_parse_timestamp_garbage_returns_none():
    assert parse_timestamp("not-a-date") is None


# --- transform_calls ---

def test_transform_calls_basic(sample_raw_call):
    result = transform_calls([sample_raw_call], SYNC_DATE)
    assert len(result) == 1
    row = result[0]
    assert row["id"] == 100001
    assert row["call_type"] == "incoming"
    assert row["billsec"] == 120
    assert row["talking_time"] == 115
    assert row["waiting_time"] == 5
    assert row["wrapup_time"] == 30
    assert row["call_status"] == "answered"
    assert row["call_date"] == date(2026, 3, 3)
    assert row["contact_name"] == "Test Customer"
    assert row["contact_company"] == "ACME d.o.o."
    assert row["contact_id"] == "999"
    assert row["user_id"] == "42"
    assert row["country_code"] == "SI"
    assert row["recorded"] is True
    assert row["is_voicemail"] is False
    assert row["is_redirected"] is False
    assert row["agent_id"] == "42"
    assert row["agent_name"] == "Jane Doe"


def test_transform_calls_missing_agent_becomes_none():
    raw = [{"Cdr": {"id": "7", "type": "incoming"}}]
    result = transform_calls(raw, SYNC_DATE)
    assert result[0]["agent_id"] is None
    assert result[0]["agent_name"] is None


def test_transform_calls_null_agent_id_becomes_none():
    raw = [{"Cdr": {"id": "8", "type": "incoming"},
            "Agent": {"id": None, "fullname": "(unknown)"}}]
    result = transform_calls(raw, SYNC_DATE)
    assert result[0]["agent_id"] is None
    assert result[0]["agent_name"] is None


def test_transform_calls_derives_missed_status():
    raw = [{"Cdr": {"id": "2", "type": "incoming", "answered_at": None,
                    "started_at": "2026-03-03T10:00:00Z"}}]
    result = transform_calls(raw, SYNC_DATE)
    assert result[0]["call_status"] == "missed"


def test_transform_calls_handles_missing_contact():
    raw = [{"Cdr": {"id": "1", "type": "incoming",
                    "started_at": "2026-03-03T10:00:00Z"}}]
    result = transform_calls(raw, SYNC_DATE)
    assert len(result) == 1
    assert result[0]["contact_name"] is None
    assert result[0]["contact_company"] is None
    assert result[0]["contact_id"] is None
    assert result[0]["agent_id"] is None
    assert result[0]["agent_name"] is None


def test_transform_calls_none_user_id_becomes_none():
    """user_id=None from API must be stored as SQL NULL, not the string 'None'."""
    raw = [{"Cdr": {"id": "9", "type": "incoming", "user_id": None}}]
    result = transform_calls(raw, SYNC_DATE)
    assert result[0]["user_id"] is None


def test_transform_calls_is_redirected_string_zero():
    """is_redirected='0' string should map to False."""
    raw = [{"Cdr": {"id": "3", "type": "outgoing", "is_redirected": "0"}}]
    result = transform_calls(raw, SYNC_DATE)
    assert result[0]["is_redirected"] is False


def test_transform_calls_derives_call_date_from_started_at():
    raw = [{"Cdr": {"id": "4", "type": "incoming",
                    "started_at": "2026-02-15T23:59:00Z"}}]
    result = transform_calls(raw, SYNC_DATE)
    # call_date should be derived from started_at, not sync_date
    assert result[0]["call_date"] == date(2026, 2, 15)


def test_transform_calls_uses_sync_date_fallback_when_no_started_at():
    raw = [{"Cdr": {"id": "5", "type": "incoming"}}]
    result = transform_calls(raw, SYNC_DATE)
    assert result[0]["call_date"] == SYNC_DATE


def test_transform_calls_empty_list():
    assert transform_calls([], SYNC_DATE) == []


def test_transform_calls_empty_external_number_becomes_none():
    raw = [{"Cdr": {"id": "6", "type": "incoming", "public_external": ""}}]
    result = transform_calls(raw, SYNC_DATE)
    assert result[0]["public_external"] is None


# --- transform_agents ---

def test_transform_agents_basic():
    raw = [{"Agent": {
        "id": "42",
        "firstname": "Jana",
        "lastname": "Novak",
        "email": "jana@example.com",
        "availability_status": "online",
        "extension": "101",
        "default_number": "+38641000001",
        "associated_numbers": ["+38641000001", "+38641000002"],
    }}]
    result = transform_agents(raw, SYNC_DATE)
    assert len(result) == 1
    row = result[0]
    assert row["id"] == "42"
    assert row["sync_date"] == SYNC_DATE
    assert row["fullname"] == "Jana Novak"
    assert row["email"] == "jana@example.com"
    assert row["associated_numbers"] == ["+38641000001", "+38641000002"]


def test_transform_agents_fullname_strips_whitespace():
    raw = [{"Agent": {"id": "1", "firstname": "Ana", "lastname": ""}}]
    result = transform_agents(raw, SYNC_DATE)
    assert result[0]["fullname"] == "Ana"


def test_transform_agents_empty_names_become_none():
    raw = [{"Agent": {"id": "2"}}]
    result = transform_agents(raw, SYNC_DATE)
    assert result[0]["firstname"] is None
    assert result[0]["lastname"] is None
    assert result[0]["fullname"] is None


def test_transform_agents_empty_list():
    assert transform_agents([], SYNC_DATE) == []


# --- transform_group_stats ---

def test_transform_group_stats_basic():
    raw = [{
        "id": "10",
        "name": "Support",
        "operators": "5",
        "answered": "50",
        "unanswered": "10",
        "abandon_rate": "16.67",
        "avg_waiting_time": "30",
        "max_waiting_time": "120",
        "avg_call_duration": "180",
        "real_time": {
            "waiting_queue": "3",
            "avg_waiting_time": "25",
            "max_waiting_time": "90",
            "avg_abandonment_time": "15",
        },
    }]
    result = transform_group_stats(raw, SYNC_DATE)
    assert len(result) == 1
    row = result[0]
    assert row["group_id"] == 10
    assert row["group_name"] == "Support"
    assert row["sync_date"] == SYNC_DATE
    assert row["operators"] == 5
    assert row["answered"] == 50
    assert row["unanswered"] == 10
    assert row["abandon_rate"] == pytest.approx(16.67)
    assert row["rt_waiting_queue"] == 3
    assert row["rt_avg_abandonment_time"] == 15


def test_transform_group_stats_missing_real_time():
    raw = [{"id": "1", "name": "Sales"}]
    result = transform_group_stats(raw, SYNC_DATE)
    assert result[0]["rt_waiting_queue"] == 0
    assert result[0]["rt_avg_waiting_time"] == 0


def test_transform_group_stats_empty_list():
    assert transform_group_stats([], SYNC_DATE) == []
