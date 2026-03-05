"""Unit tests for ETL transform functions."""
import pytest
from datetime import date

from cloudtalk_etl.etl.transform import (
    transform_calls,
    transform_agents,
    transform_group_stats,
    transform_numbers,
    transform_groups_dim,
    transform_tags,
    build_number_lookup,
    transform_call_tags,
    transform_call_center_daily_stats,
    transform_agent_daily_stats,
    transform_call_reasons_daily,
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


# ===========================================================================
# Phase 2: Dimension transforms
# ===========================================================================

# --- transform_numbers ---

def test_transform_numbers_basic():
    # API wraps each record in a "CallNumber" key
    raw = [{"CallNumber": {"id": "7", "internal_name": "Reklamacije SLO",
                           "caller_id_e164": "+38612345678", "country_code": "386",
                           "connected_to": "4"}}]
    result = transform_numbers(raw)
    assert len(result) == 1
    row = result[0]
    assert row["id"] == 7
    assert row["internal_name"] == "Reklamacije SLO"
    assert row["caller_id_e164"] == "+38612345678"
    assert row["country_code"] == 386
    assert row["connected_to"] == 4
    assert row["source_id"] is None  # not provided by API


def test_transform_numbers_connected_to_is_preserved():
    raw = [{"CallNumber": {"id": "9", "internal_name": "Direct",
                           "caller_id_e164": "+38641000001",
                           "country_code": "386", "connected_to": "1"}}]
    result = transform_numbers(raw)
    assert result[0]["connected_to"] == 1


def test_transform_numbers_missing_connected_to_becomes_none():
    raw = [{"CallNumber": {"id": "5", "internal_name": "X"}}]
    result = transform_numbers(raw)
    assert result[0]["connected_to"] is None
    assert result[0]["source_id"] is None
    assert result[0]["caller_id_e164"] is None


def test_transform_numbers_empty_list():
    assert transform_numbers([]) == []


# --- transform_groups_dim ---

def test_transform_groups_dim_basic():
    # API wraps each record in a "Group" key
    raw = [{"Group": {"id": "10", "internal_name": "Reklamacije SLO"}},
           {"Group": {"id": "11", "internal_name": "Svetovanje HR"}}]
    result = transform_groups_dim(raw)
    assert len(result) == 2
    assert result[0] == {"id": 10, "internal_name": "Reklamacije SLO"}
    assert result[1] == {"id": 11, "internal_name": "Svetovanje HR"}


def test_transform_groups_dim_skips_placeholder():
    raw = [{"Group": {"id": "0", "internal_name": None}},
           {"Group": {"id": "10", "internal_name": "Support"}}]
    result = transform_groups_dim(raw)
    assert len(result) == 1
    assert result[0]["id"] == 10


def test_transform_groups_dim_empty_list():
    assert transform_groups_dim([]) == []


# --- transform_tags ---

def test_transform_tags_basic():
    # API wraps each record in a "Tag" key
    raw = [{"Tag": {"id": "5", "name": "REKLAMACIJE"}},
           {"Tag": {"id": "13", "name": "SVETOVANJE PRI PRODAJI"}}]
    result = transform_tags(raw)
    assert len(result) == 2
    assert result[0] == {"id": 5, "name": "REKLAMACIJE"}
    assert result[1] == {"id": 13, "name": "SVETOVANJE PRI PRODAJI"}


def test_transform_tags_skips_placeholder():
    raw = [{"Tag": {"id": "0", "name": None}},
           {"Tag": {"id": "5", "name": "REKLAMACIJE"}}]
    result = transform_tags(raw)
    assert len(result) == 1
    assert result[0]["id"] == 5


def test_transform_tags_empty_list():
    assert transform_tags([]) == []


# --- build_number_lookup ---

def test_build_number_lookup_group_mapped(sample_numbers, sample_groups_dim):
    lookup = build_number_lookup(sample_numbers, sample_groups_dim)
    assert 7 in lookup
    assert lookup[7]["group_id"] == 10
    assert lookup[7]["group_name"] == "Reklamacije SLO"
    assert lookup[7]["country_code"] == 386


def test_build_number_lookup_agent_number_has_no_group(sample_numbers, sample_groups_dim):
    lookup = build_number_lookup(sample_numbers, sample_groups_dim)
    # number 9 is connected_to=1 (agent), so group_id should be None
    assert lookup[9]["group_id"] is None
    assert lookup[9]["group_name"] is None


def test_build_number_lookup_two_countries(sample_numbers, sample_groups_dim):
    lookup = build_number_lookup(sample_numbers, sample_groups_dim)
    assert lookup[7]["country_code"] == 386   # SLO
    assert lookup[8]["country_code"] == 385   # HR


def test_build_number_lookup_empty_inputs():
    assert build_number_lookup([], []) == {}


# ===========================================================================
# Phase 2: Fact / aggregation transforms
# ===========================================================================

# --- transform_call_tags ---

def test_transform_call_tags_basic(sample_raw_call_with_tags):
    result = transform_call_tags([sample_raw_call_with_tags])
    assert len(result) == 2
    call_ids = {r["call_id"] for r in result}
    tag_ids = {r["tag_id"] for r in result}
    assert call_ids == {100001}
    assert tag_ids == {5, 13}
    names = {r["tag_name"] for r in result}
    assert "REKLAMACIJE" in names
    assert "SVETOVANJE PRI PRODAJI" in names


def test_transform_call_tags_no_tags(sample_raw_call_missed):
    result = transform_call_tags([sample_raw_call_missed])
    assert result == []


def test_transform_call_tags_multiple_calls(sample_raw_call_with_tags):
    call2 = {**sample_raw_call_with_tags,
             "Cdr": {**sample_raw_call_with_tags["Cdr"], "id": "100002"},
             "Tags": [{"id": "5", "name": "REKLAMACIJE"}]}
    result = transform_call_tags([sample_raw_call_with_tags, call2])
    assert len(result) == 3   # 2 tags from call1 + 1 from call2


def test_transform_call_tags_empty_list():
    assert transform_call_tags([]) == []


# --- transform_call_center_daily_stats ---

def test_transform_call_center_daily_stats_answered(sample_raw_call_with_tags):
    # CallNumber.id=7 used directly as group_id; internal_name and country_code from CallNumber
    result = transform_call_center_daily_stats([sample_raw_call_with_tags], SYNC_DATE)
    assert len(result) == 1
    row = result[0]
    assert row["group_id"] == 7  # CallNumber.id
    assert row["group_name"] == "Reklamacije SLO"  # CallNumber.internal_name
    assert row["country_code"] == 386  # CallNumber.country_code
    assert row["total_calls"] == 1
    assert row["answered_calls"] == 1
    assert row["missed_calls"] == 0
    assert row["answer_rate_pct"] == 100.0
    assert row["callback_calls"] == 0
    assert row["sync_date"] == SYNC_DATE


def test_transform_call_center_daily_stats_missed(sample_raw_call_missed):
    result = transform_call_center_daily_stats([sample_raw_call_missed], SYNC_DATE)
    assert len(result) == 1
    row = result[0]
    assert row["answered_calls"] == 0
    assert row["missed_calls"] == 1
    assert row["answer_rate_pct"] == 0.0


def test_transform_call_center_daily_stats_answer_rate(
        sample_raw_call_with_tags, sample_raw_call_missed):
    result = transform_call_center_daily_stats(
        [sample_raw_call_with_tags, sample_raw_call_missed], SYNC_DATE
    )
    row = result[0]
    assert row["total_calls"] == 2
    assert row["answered_calls"] == 1
    assert row["answer_rate_pct"] == 50.0


def test_transform_call_center_daily_stats_skips_call_without_number():
    call = {
        "Cdr": {"id": "9999", "answered_at": "2026-03-03T09:00:00Z"},
        "Tags": [],
    }
    result = transform_call_center_daily_stats([call], SYNC_DATE)
    assert result == []


def test_transform_call_center_daily_stats_empty_list():
    assert transform_call_center_daily_stats([], SYNC_DATE) == []


# --- transform_agent_daily_stats ---

def test_transform_agent_daily_stats_basic(sample_raw_call_with_tags):
    result = transform_agent_daily_stats([sample_raw_call_with_tags], SYNC_DATE)
    assert len(result) == 1
    row = result[0]
    assert row["agent_id"] == 42
    assert row["agent_name"] == "Jane Doe"
    assert row["answered_calls"] == 1
    assert row["total_talk_seconds"] == 115
    assert row["presented_calls"] == 0
    assert row["sync_date"] == SYNC_DATE


def test_transform_agent_daily_stats_skips_null_user_id(sample_raw_call_missed):
    result = transform_agent_daily_stats([sample_raw_call_missed], SYNC_DATE)
    assert result == []


def test_transform_agent_daily_stats_aggregates_multiple_calls(sample_raw_call_with_tags):
    call2 = {
        "Cdr": {**sample_raw_call_with_tags["Cdr"],
                "id": "100003", "talking_time": "60",
                "answered_at": "2026-03-03T10:00:00Z"},
        "Agent": {"id": "42", "fullname": "Jane Doe"},
        "CallNumber": {"id": "7"},
        "Tags": [],
    }
    result = transform_agent_daily_stats([sample_raw_call_with_tags, call2], SYNC_DATE)
    assert len(result) == 1
    row = result[0]
    assert row["answered_calls"] == 2
    assert row["total_talk_seconds"] == 175  # 115 + 60


def test_transform_agent_daily_stats_two_agents(sample_raw_call_with_tags):
    call2 = {
        "Cdr": {"id": "100004", "talking_time": "90",
                "answered_at": "2026-03-03T11:00:00Z", "user_id": "99"},
        "Agent": {"id": "99", "fullname": "Marko Horvat"},
        "CallNumber": {"id": "7"},
        "Tags": [],
    }
    result = transform_agent_daily_stats([sample_raw_call_with_tags, call2], SYNC_DATE)
    assert len(result) == 2
    agent_ids = {r["agent_id"] for r in result}
    assert agent_ids == {42, 99}


def test_transform_agent_daily_stats_empty_list():
    assert transform_agent_daily_stats([], SYNC_DATE) == []


# --- transform_call_reasons_daily ---

def test_transform_call_reasons_daily_basic(sample_raw_call_with_tags):
    # CallNumber.id=7 used as group_id; group_name from CallNumber.internal_name
    result = transform_call_reasons_daily([sample_raw_call_with_tags], SYNC_DATE)
    assert len(result) == 2
    tag_ids = {r["tag_id"] for r in result}
    assert tag_ids == {5, 13}
    for row in result:
        assert row["group_id"] == 7  # CallNumber.id
        assert row["group_name"] == "Reklamacije SLO"  # CallNumber.internal_name
        assert row["call_count"] == 1
        assert row["sync_date"] == SYNC_DATE


def test_transform_call_reasons_daily_accumulates_counts(sample_raw_call_with_tags):
    # Two calls both tagged REKLAMACIJE
    call2 = {**sample_raw_call_with_tags,
             "Cdr": {**sample_raw_call_with_tags["Cdr"], "id": "100099"},
             "Tags": [{"id": "5", "name": "REKLAMACIJE"}]}
    result = transform_call_reasons_daily([sample_raw_call_with_tags, call2], SYNC_DATE)
    reklamacije = next(r for r in result if r["tag_id"] == 5)
    assert reklamacije["call_count"] == 2


def test_transform_call_reasons_daily_skips_calls_without_number():
    call = {
        "Cdr": {"id": "7777"},
        # No CallNumber key
        "Tags": [{"id": "5", "name": "REKLAMACIJE"}],
    }
    result = transform_call_reasons_daily([call], SYNC_DATE)
    assert result == []


def test_transform_call_reasons_daily_no_tags(sample_raw_call_missed):
    result = transform_call_reasons_daily([sample_raw_call_missed], SYNC_DATE)
    assert result == []


def test_transform_call_reasons_daily_empty_list():
    assert transform_call_reasons_daily([], SYNC_DATE) == []
