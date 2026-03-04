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
    }
