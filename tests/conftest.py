# tests/conftest.py
"""Shared pytest fixtures for the cloudtalk-etl test suite."""
import pytest


@pytest.fixture
def sample_raw_call() -> dict:
    """A minimal raw call record as returned by /calls/index.json."""
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


@pytest.fixture
def sample_call_detail_answered() -> dict:
    """
    A call detail response from GET /calls/{callId} (analytics API).

    Represents an answered call in the 'Reklamacije - SLO' queue,
    with one agent who was presented and answered.
    """
    return {
        "cdr_id": 100001,
        "uuid": "abc-123",
        "date": "2026-03-03T09:00:00Z",
        "status": "answered",
        "direction": "incoming",
        "type": "regular",
        "contact": {"id": 999, "name": "Test Customer", "country": "SI", "number": "+38641000001"},
        "internal_number": {"id": 7, "name": "Reklamacije - SLO", "number": "+38612345678"},
        "call_tags": [
            {"id": 5, "label": "REKLAMACIJE"},
        ],
        "call_times": {"total_time": 120, "talking_time": 115, "ringing_time": 5},
        "call_steps": [
            {
                "type": "ivr",
                "name": "Main IVR",
                "date": "2026-03-03T09:00:00Z",
            },
            {
                "type": "queue",
                "id": 10,
                "name": "Reklamacije - SLO",
                "status": "answered",
                "strategy": "rrmemory",
                "date": "2026-03-03T09:00:02Z",
                "call_times": {"total_time": 118, "talking_time": 115, "ringing_time": 3},
                "agent_calls": [
                    {
                        "type": "agent",
                        "id": 42,
                        "name": "Jane Doe",
                        "status": "answered",
                        "date": "2026-03-03T09:00:05Z",
                        "call_times": {
                            "total_time": 115,
                            "talking_time": 115,
                            "ringing_time": 3,
                            "waiting_time": 2,
                            "holding_time": 0,
                            "wrap_up_time": 0,
                        },
                        "group_ids": [10],
                    }
                ],
            },
        ],
        "notes": [],
        "recorded": True,
        "out_of_office": False,
    }


@pytest.fixture
def sample_call_detail_missed() -> dict:
    """
    A call detail response for a missed call (no agent answered).
    The queue step has status='missed' with an agent who was tried but missed.
    """
    return {
        "cdr_id": 100002,
        "uuid": "def-456",
        "date": "2026-03-03T10:00:00Z",
        "status": "missed",
        "direction": "incoming",
        "type": "regular",
        "contact": {"id": 0, "name": None, "country": "SI", "number": "+38641000002"},
        "internal_number": {"id": 7, "name": "Reklamacije - SLO", "number": "+38612345678"},
        "call_tags": [],
        "call_times": {"total_time": 30, "talking_time": 0, "ringing_time": 30},
        "call_steps": [
            {
                "type": "queue",
                "id": 10,
                "name": "Reklamacije - SLO",
                "status": "missed",
                "strategy": "rrmemory",
                "date": "2026-03-03T10:00:00Z",
                "call_times": {"total_time": 30, "talking_time": 0, "ringing_time": 30},
                "agent_calls": [
                    {
                        "type": "agent",
                        "id": 42,
                        "name": "Jane Doe",
                        "status": "missed",
                        "date": "2026-03-03T10:00:05Z",
                        "call_times": {
                            "total_time": 25,
                            "talking_time": 0,
                            "ringing_time": 25,
                            "waiting_time": 0,
                            "holding_time": 0,
                            "wrap_up_time": 0,
                        },
                        "group_ids": [10],
                        "reason": "not_picked_up",
                    }
                ],
            },
        ],
        "notes": [],
        "recorded": False,
        "out_of_office": False,
    }


@pytest.fixture
def sample_call_detail_with_tags() -> dict:
    """A call detail with multiple tags attached, from a CRO group."""
    return {
        "cdr_id": 100003,
        "uuid": "ghi-789",
        "date": "2026-03-03T11:00:00Z",
        "status": "answered",
        "direction": "incoming",
        "type": "regular",
        "contact": {"id": 1001, "name": "Another Customer", "country": "HR", "number": "+38591000001"},
        "internal_number": {"id": 8, "name": "Reklamacije - CRO", "number": "+38512345678"},
        "call_tags": [
            {"id": 5, "label": "REKLAMACIJE"},
            {"id": 13, "label": "SVETOVANJE PRI PRODAJI"},
        ],
        "call_times": {"total_time": 200, "talking_time": 180, "ringing_time": 20},
        "call_steps": [
            {
                "type": "queue",
                "id": 20,
                "name": "Reklamacije - CRO",
                "status": "answered",
                "strategy": "rrmemory",
                "date": "2026-03-03T11:00:05Z",
                "call_times": {"total_time": 180, "talking_time": 180, "ringing_time": 0},
                "agent_calls": [
                    {
                        "type": "agent",
                        "id": 99,
                        "name": "Marko Horvat",
                        "status": "answered",
                        "date": "2026-03-03T11:00:10Z",
                        "call_times": {
                            "total_time": 180,
                            "talking_time": 180,
                            "ringing_time": 5,
                            "waiting_time": 5,
                            "holding_time": 0,
                            "wrap_up_time": 0,
                        },
                        "group_ids": [20],
                    }
                ],
            },
        ],
        "notes": [],
        "recorded": True,
        "out_of_office": False,
    }


@pytest.fixture
def sample_call_detail_no_queue() -> dict:
    """A call detail with no queue step — uses internal_number as fallback."""
    return {
        "cdr_id": 100004,
        "uuid": "jkl-012",
        "date": "2026-03-03T12:00:00Z",
        "status": "missed",
        "direction": "incoming",
        "type": "regular",
        "contact": {"id": 0, "name": None, "country": "SI", "number": "+38641000003"},
        "internal_number": {"id": 9, "name": "Info - SLO", "number": "+38612000000"},
        "call_tags": [],
        "call_times": {"total_time": 5, "talking_time": 0, "ringing_time": 5},
        "call_steps": [
            {"type": "ivr", "name": "Auto-attendant"},
        ],
        "notes": [],
        "recorded": False,
        "out_of_office": False,
    }
