"""Unit tests for ETL transform functions."""
import pytest
from datetime import date

from cloudtalk_etl.etl.transform import (
    safe_int,
    safe_float,
    parse_timestamp,
    format_date_eu,
    parse_group_name,
    transform_call_center_groups,
    transform_agent_stats,
    transform_call_reasons,
)

SYNC_DATE = date(2026, 3, 3)


# ===========================================================================
# Utility helpers
# ===========================================================================

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


def test_safe_float_valid_string():
    assert safe_float("3.14") == pytest.approx(3.14)


def test_safe_float_none_returns_default():
    assert safe_float(None) == 0.0


def test_safe_float_garbage_returns_default():
    assert safe_float("bad") == 0.0


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


# ===========================================================================
# format_date_eu
# ===========================================================================

def test_format_date_eu_basic():
    assert format_date_eu(date(2026, 3, 3)) == "03.03.2026"


def test_format_date_eu_single_digit_day_month():
    assert format_date_eu(date(2026, 1, 5)) == "05.01.2026"


def test_format_date_eu_end_of_year():
    assert format_date_eu(date(2025, 12, 31)) == "31.12.2025"


# ===========================================================================
# parse_group_name
# ===========================================================================

def test_parse_group_name_standard():
    country, category = parse_group_name("Reklamacije - SLO")
    assert country == "SLO"
    assert category == "Reklamacije"


def test_parse_group_name_cro():
    country, category = parse_group_name("Svetovanje - CRO")
    assert country == "CRO"
    assert category == "Svetovanje"


def test_parse_group_name_multi_word_category():
    country, category = parse_group_name("Svetovanje pri prodaji - SLO")
    assert country == "SLO"
    assert category == "Svetovanje pri prodaji"


def test_parse_group_name_parenthesis_prefix_slo():
    country, category = parse_group_name("(SLO) Contact Center Number")
    assert country == "SLO"
    assert category == "Contact Center Number"


def test_parse_group_name_parenthesis_prefix_cro():
    country, category = parse_group_name("(CRO) Contact Center Number")
    assert country == "CRO"
    assert category == "Contact Center Number"


def test_parse_group_name_no_delimiter_returns_unknown():
    country, category = parse_group_name("SomeGroupWithoutDelimiter")
    assert country == "UNKNOWN"
    assert category == "SomeGroupWithoutDelimiter"


def test_parse_group_name_empty_string_returns_unknown():
    country, category = parse_group_name("")
    assert country == "UNKNOWN"


def test_parse_group_name_strips_whitespace():
    country, category = parse_group_name("Info  -  SLO")
    assert country == "SLO"
    assert category == "Info"


# ===========================================================================
# transform_call_center_groups
# ===========================================================================

def test_transform_call_center_groups_answered(sample_call_detail_answered):
    details = {100001: sample_call_detail_answered}
    result = transform_call_center_groups(details, SYNC_DATE)

    assert len(result) == 1
    row = result[0]
    assert row["date"] == "03.03.2026"
    assert row["group_name"] == "Reklamacije - SLO"
    assert row["country"] == "SLO"
    assert row["category"] == "Reklamacije"
    assert row["total_calls"] == 1
    assert row["answered_calls"] == 1
    assert row["unanswered_calls"] == 0
    assert row["answered_pct"] == pytest.approx(100.0)


def test_transform_call_center_groups_missed(sample_call_detail_missed):
    details = {100002: sample_call_detail_missed}
    result = transform_call_center_groups(details, SYNC_DATE)

    assert len(result) == 1
    row = result[0]
    assert row["total_calls"] == 1
    assert row["answered_calls"] == 0
    assert row["unanswered_calls"] == 1
    assert row["answered_pct"] == pytest.approx(0.0)


def test_transform_call_center_groups_aggregates_multiple_calls(
    sample_call_detail_answered, sample_call_detail_missed
):
    details = {
        100001: sample_call_detail_answered,
        100002: sample_call_detail_missed,
    }
    result = transform_call_center_groups(details, SYNC_DATE)

    assert len(result) == 1
    row = result[0]
    assert row["total_calls"] == 2
    assert row["answered_calls"] == 1
    assert row["unanswered_calls"] == 1
    assert row["answered_pct"] == pytest.approx(50.0)


def test_transform_call_center_groups_two_groups(
    sample_call_detail_answered, sample_call_detail_with_tags
):
    details = {
        100001: sample_call_detail_answered,    # Reklamacije - SLO
        100003: sample_call_detail_with_tags,   # Reklamacije - CRO
    }
    result = transform_call_center_groups(details, SYNC_DATE)

    assert len(result) == 2
    groups = {r["group_name"]: r for r in result}
    assert "Reklamacije - SLO" in groups
    assert "Reklamacije - CRO" in groups
    assert groups["Reklamacije - CRO"]["country"] == "CRO"


def test_transform_call_center_groups_fallback_to_internal_number(
    sample_call_detail_no_queue
):
    details = {100004: sample_call_detail_no_queue}
    result = transform_call_center_groups(details, SYNC_DATE)

    assert len(result) == 1
    assert result[0]["group_name"] == "Info - SLO"
    assert result[0]["country"] == "SLO"
    assert result[0]["category"] == "Info"


def test_transform_call_center_groups_empty_details():
    assert transform_call_center_groups({}, SYNC_DATE) == []


def test_transform_call_center_groups_date_format(sample_call_detail_answered):
    details = {100001: sample_call_detail_answered}
    result = transform_call_center_groups(details, date(2026, 1, 5))
    assert result[0]["date"] == "05.01.2026"


# ===========================================================================
# transform_agent_stats
# ===========================================================================

def test_transform_agent_stats_answered(sample_call_detail_answered):
    details = {100001: sample_call_detail_answered}
    result = transform_agent_stats(details, SYNC_DATE)

    assert len(result) == 1
    row = result[0]
    assert row["date"] == "03.03.2026"
    assert row["agent_id"] == 42
    assert row["agent_name"] == "Jane Doe"
    assert row["group_name"] == "Reklamacije - SLO"
    assert row["country"] == "SLO"
    assert row["category"] == "Reklamacije"
    assert row["presented_calls"] == 1
    assert row["answered_calls"] == 1
    assert row["talking_time_sec"] == 115


def test_transform_agent_stats_missed_agent(sample_call_detail_missed):
    details = {100002: sample_call_detail_missed}
    result = transform_agent_stats(details, SYNC_DATE)

    assert len(result) == 1
    row = result[0]
    assert row["agent_id"] == 42
    assert row["presented_calls"] == 1
    assert row["answered_calls"] == 0
    assert row["talking_time_sec"] == 0


def test_transform_agent_stats_aggregates_across_calls(
    sample_call_detail_answered, sample_call_detail_missed
):
    details = {
        100001: sample_call_detail_answered,
        100002: sample_call_detail_missed,
    }
    result = transform_agent_stats(details, SYNC_DATE)

    assert len(result) == 1
    row = result[0]
    assert row["presented_calls"] == 2
    assert row["answered_calls"] == 1
    assert row["talking_time_sec"] == 115


def test_transform_agent_stats_two_agents(
    sample_call_detail_answered, sample_call_detail_with_tags
):
    details = {
        100001: sample_call_detail_answered,    # agent 42
        100003: sample_call_detail_with_tags,   # agent 99
    }
    result = transform_agent_stats(details, SYNC_DATE)

    assert len(result) == 2
    agent_ids = {r["agent_id"] for r in result}
    assert agent_ids == {42, 99}


def test_transform_agent_stats_empty_details():
    assert transform_agent_stats({}, SYNC_DATE) == []


def test_transform_agent_stats_no_queue_step_produces_no_rows(
    sample_call_detail_no_queue
):
    details = {100004: sample_call_detail_no_queue}
    result = transform_agent_stats(details, SYNC_DATE)
    assert result == []


# ===========================================================================
# transform_call_reasons
# ===========================================================================

def test_transform_call_reasons_with_tags(sample_call_detail_answered):
    details = {100001: sample_call_detail_answered}
    result = transform_call_reasons(details, SYNC_DATE)

    assert len(result) == 1
    row = result[0]
    assert row["date"] == "03.03.2026"
    assert row["tag_id"] == 5
    assert row["tag_name"] == "REKLAMACIJE"
    assert row["group_name"] == "Reklamacije - SLO"
    assert row["country"] == "SLO"
    assert row["category"] == "Reklamacije"
    assert row["call_count"] == 1


def test_transform_call_reasons_multiple_tags(sample_call_detail_with_tags):
    details = {100003: sample_call_detail_with_tags}
    result = transform_call_reasons(details, SYNC_DATE)

    assert len(result) == 2
    tag_ids = {r["tag_id"] for r in result}
    assert tag_ids == {5, 13}
    tag_names = {r["tag_name"] for r in result}
    assert "REKLAMACIJE" in tag_names
    assert "SVETOVANJE PRI PRODAJI" in tag_names


def test_transform_call_reasons_no_tags(sample_call_detail_missed):
    details = {100002: sample_call_detail_missed}
    result = transform_call_reasons(details, SYNC_DATE)
    assert result == []


def test_transform_call_reasons_accumulates_counts(sample_call_detail_answered):
    call2 = {
        **sample_call_detail_answered,
        "cdr_id": 100099,
        "call_tags": [{"id": 5, "label": "REKLAMACIJE"}],
    }
    details = {
        100001: sample_call_detail_answered,
        100099: call2,
    }
    result = transform_call_reasons(details, SYNC_DATE)

    assert len(result) == 1
    assert result[0]["call_count"] == 2


def test_transform_call_reasons_uses_label_field(sample_call_detail_answered):
    """Tags in detail response use 'label', not 'name'."""
    result = transform_call_reasons({100001: sample_call_detail_answered}, SYNC_DATE)
    assert result[0]["tag_name"] == "REKLAMACIJE"


def test_transform_call_reasons_empty_details():
    assert transform_call_reasons({}, SYNC_DATE) == []
