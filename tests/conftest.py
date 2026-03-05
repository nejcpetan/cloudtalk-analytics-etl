# tests/conftest.py
"""Shared pytest fixtures for the cloudtalk-etl test suite."""
import pytest


@pytest.fixture
def sample_raw_call() -> dict:
    """A minimal raw call record as returned by the CloudTalk API."""
    return {
        "Cdr": {
            "id": "100001",
            "type": "incoming",
            "billsec": "120",
            "talking_time": "115",
            "waiting_time": "5",
            "wrapup_time": "30",
            "public_external": "+38641000001",
            "public_internal": "101",
            "country_code": "SI",
            "recorded": True,
            "is_voicemail": False,
            "is_redirected": "0",
            "redirected_from": None,
            "user_id": "42",
            "started_at": "2026-03-03T09:00:00Z",
            "answered_at": "2026-03-03T09:00:05Z",
            "ended_at": "2026-03-03T09:02:00Z",
            "recording_link": "https://example.cloudtalk.io/recordings/100001.mp3",
        },
        "Contact": {
            "id": "999",
            "name": "Test Customer",
            "company": "ACME d.o.o.",
        },
        "Agent": {
            "id": "42",
            "fullname": "Jane Doe",
        },
    }


# ---------------------------------------------------------------------------
# Phase 2 fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_raw_call_with_tags() -> dict:
    """Raw call record including Tags and CallNumber, for Phase 2 transforms."""
    return {
        "Cdr": {
            "id": "100001",
            "type": "incoming",
            "talking_time": "115",
            "answered_at": "2026-03-03T09:00:05Z",
            "user_id": "42",
        },
        "Agent": {"id": "42", "fullname": "Jane Doe"},
        "Contact": {"id": "999", "name": "Test Customer"},
        "CallNumber": {
            "id": "7",
            "caller_id_e164": "+38612345678",
            "internal_name": "Reklamacije SLO",
            "country_code": "386",
        },
        "Tags": [
            {"id": "5", "name": "REKLAMACIJE"},
            {"id": "13", "name": "SVETOVANJE PRI PRODAJI"},
        ],
    }


@pytest.fixture
def sample_raw_call_missed() -> dict:
    """A missed call with no agent and no tags."""
    return {
        "Cdr": {
            "id": "100002",
            "type": "incoming",
            "talking_time": "0",
            "answered_at": None,
            "user_id": None,
        },
        "Agent": {},
        "Contact": {},
        "CallNumber": {
            "id": "7",
            "caller_id_e164": "+38612345678",
            "internal_name": "Reklamacije SLO",
            "country_code": "386",
        },
        "Tags": [],
    }


@pytest.fixture
def sample_numbers() -> list[dict]:
    """Transformed number records (post transform_numbers)."""
    return [
        {
            "id": 7,
            "internal_name": "Reklamacije SLO",
            "caller_id_e164": "+38612345678",
            "country_code": 386,
            "connected_to": 0,
            "source_id": 10,
        },
        {
            "id": 8,
            "internal_name": "Svetovanje HR",
            "caller_id_e164": "+38512345678",
            "country_code": 385,
            "connected_to": 0,
            "source_id": 11,
        },
        {
            "id": 9,
            "internal_name": "Direct Agent Line",
            "caller_id_e164": "+38641999999",
            "country_code": 386,
            "connected_to": 1,   # agent, not group
            "source_id": None,
        },
    ]


@pytest.fixture
def sample_groups_dim() -> list[dict]:
    """Transformed group records (post transform_groups_dim)."""
    return [
        {"id": 10, "internal_name": "Reklamacije SLO"},
        {"id": 11, "internal_name": "Svetovanje HR"},
    ]


@pytest.fixture
def sample_number_lookup(sample_numbers, sample_groups_dim):
    """Pre-built number lookup dict."""
    from cloudtalk_etl.etl.transform import build_number_lookup
    return build_number_lookup(sample_numbers, sample_groups_dim)
