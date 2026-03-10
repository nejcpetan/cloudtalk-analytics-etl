# tests/test_etl.py
"""
End-to-end ETL pipeline tests.

Tests the extract → transform → load flow using mocked API responses
and a mocked database connection.
"""
import pytest
from datetime import date
from unittest.mock import MagicMock, patch

from cloudtalk_etl.etl.extract import extract_call_details


SYNC_DATE = date(2026, 3, 3)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_raw_calls(call_ids: list[int]) -> list[dict]:
    return [{"Cdr": {"id": str(cid)}} for cid in call_ids]


# ---------------------------------------------------------------------------
# extract_call_details
# ---------------------------------------------------------------------------

def test_extract_call_details_fetches_each_call(sample_call_detail_answered):
    """extract_call_details should call get_call_detail() once per call ID."""
    client = MagicMock()
    client.get_call_detail.return_value = sample_call_detail_answered

    raw_calls = _make_raw_calls([100001, 100002])

    with patch("cloudtalk_etl.etl.extract.time.sleep"):
        result = extract_call_details(client, raw_calls)

    assert client.get_call_detail.call_count == 2
    assert 100001 in result
    assert 100002 in result


def test_extract_call_details_skips_failed_calls(sample_call_detail_answered):
    """Failed detail fetches should be skipped, not raise."""
    client = MagicMock()

    def side_effect(call_id):
        if call_id == 100002:
            raise Exception("API error")
        return sample_call_detail_answered

    client.get_call_detail.side_effect = side_effect
    raw_calls = _make_raw_calls([100001, 100002])

    with patch("cloudtalk_etl.etl.extract.time.sleep"):
        result = extract_call_details(client, raw_calls)

    assert 100001 in result
    assert 100002 not in result


def test_extract_call_details_respects_test_mode(sample_call_detail_answered):
    """In test_mode, only first 10 call IDs should be fetched."""
    client = MagicMock()
    client.get_call_detail.return_value = sample_call_detail_answered

    raw_calls = _make_raw_calls(list(range(100001, 100021)))  # 20 calls

    with patch("cloudtalk_etl.etl.extract.time.sleep"):
        result = extract_call_details(client, raw_calls, test_mode=True)

    assert client.get_call_detail.call_count == 10
    assert len(result) == 10


def test_extract_call_details_returns_empty_for_no_calls():
    client = MagicMock()
    result = extract_call_details(client, [])
    assert result == {}
    client.get_call_detail.assert_not_called()


def test_extract_call_details_skips_invalid_cdr_ids():
    """Records with missing or non-integer Cdr.id should be silently skipped."""
    client = MagicMock()
    client.get_call_detail.return_value = {"cdr_id": 100001, "call_steps": []}

    raw_calls = [
        {"Cdr": {"id": "100001"}},
        {"Cdr": {}},              # missing id
        {"Cdr": {"id": "abc"}},   # non-integer
    ]

    with patch("cloudtalk_etl.etl.extract.time.sleep"):
        result = extract_call_details(client, raw_calls)

    assert client.get_call_detail.call_count == 1
    assert 100001 in result


def test_extract_call_details_sleeps_between_requests(sample_call_detail_answered):
    """There should be a sleep between detail requests (1050ms throttle)."""
    client = MagicMock()
    client.get_call_detail.return_value = sample_call_detail_answered

    raw_calls = _make_raw_calls([100001, 100002, 100003])

    with patch("cloudtalk_etl.etl.extract.time.sleep") as mock_sleep:
        extract_call_details(client, raw_calls)

    # sleep should be called N-1 times (no sleep before first request)
    assert mock_sleep.call_count == 2
    # Each sleep should be ~1.05 seconds
    for sleep_call in mock_sleep.call_args_list:
        assert sleep_call.args[0] == pytest.approx(1.05)
