import structlog
from datetime import date, datetime

logger = structlog.get_logger()


def safe_int(value, default: int = 0) -> int:
    """Safely convert a value to int."""
    try:
        return int(value) if value is not None else default
    except (ValueError, TypeError):
        return default


def safe_float(value, default: float = 0.0) -> float:
    """Safely convert a value to float."""
    try:
        return float(value) if value is not None else default
    except (ValueError, TypeError):
        return default


def parse_timestamp(value: str | None) -> str | None:
    """Parse and validate a timestamp string. Returns None if invalid."""
    if not value or value == "" or value == "0":
        return None
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
        return value
    except (ValueError, AttributeError):
        return None


def transform_calls(raw_calls: list[dict], sync_date: date) -> list[dict]:
    """
    Transform raw CloudTalk call data into flat dictionaries
    ready for database insertion.
    """
    transformed = []

    for record in raw_calls:
        cdr = record.get("Cdr", {})
        contact = record.get("Contact", {})

        answered_at = parse_timestamp(cdr.get("answered_at"))
        call_status = "answered" if answered_at else "missed"

        started_at = parse_timestamp(cdr.get("started_at"))
        call_date = sync_date  # Use sync_date as fallback

        if started_at:
            try:
                dt = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
                call_date = dt.date()
            except ValueError:
                pass

        transformed.append({
            "id": safe_int(cdr.get("id")),
            "call_type": cdr.get("type", "unknown"),
            "billsec": safe_int(cdr.get("billsec")),
            "talking_time": safe_int(cdr.get("talking_time")),
            "waiting_time": safe_int(cdr.get("waiting_time")),
            "wrapup_time": safe_int(cdr.get("wrapup_time")),
            "public_external": str(cdr.get("public_external", "")) or None,
            "public_internal": str(cdr.get("public_internal", "")) or None,
            "country_code": cdr.get("country_code"),
            "recorded": bool(cdr.get("recorded", False)),
            "is_voicemail": bool(cdr.get("is_voicemail", False)),
            "is_redirected": bool(cdr.get("is_redirected", False)
                                  if cdr.get("is_redirected") != "0" else False),
            "redirected_from": cdr.get("redirected_from") or None,
            "user_id": str(cdr.get("user_id", "")) or None,
            "started_at": started_at,
            "answered_at": answered_at,
            "ended_at": parse_timestamp(cdr.get("ended_at")),
            "recording_link": cdr.get("recording_link"),
            "call_status": call_status,
            "call_date": call_date,
            "contact_id": str(contact.get("id", "")) or None,
            "contact_name": contact.get("name") or None,
            "contact_company": contact.get("company") or None,
        })

    logger.info("calls_transformed", count=len(transformed))
    return transformed


def transform_agents(raw_agents: list[dict], sync_date: date) -> list[dict]:
    """Transform raw CloudTalk agent data."""
    transformed = []

    for record in raw_agents:
        agent = record.get("Agent", {})
        firstname = agent.get("firstname", "")
        lastname = agent.get("lastname", "")

        transformed.append({
            "id": str(agent.get("id", "")),
            "sync_date": sync_date,
            "firstname": firstname or None,
            "lastname": lastname or None,
            "fullname": f"{firstname} {lastname}".strip() or None,
            "email": agent.get("email") or None,
            "availability_status": agent.get("availability_status") or None,
            "extension": agent.get("extension") or None,
            "default_number": agent.get("default_number") or None,
            "associated_numbers": agent.get("associated_numbers", []),
        })

    logger.info("agents_transformed", count=len(transformed))
    return transformed


def transform_group_stats(raw_stats: list[dict], sync_date: date) -> list[dict]:
    """Transform raw CloudTalk group statistics."""
    transformed = []

    for group in raw_stats:
        real_time = group.get("real_time", {})

        transformed.append({
            "group_id": safe_int(group.get("id")),
            "group_name": group.get("name", "Unknown"),
            "sync_date": sync_date,
            "operators": safe_int(group.get("operators")),
            "answered": safe_int(group.get("answered")),
            "unanswered": safe_int(group.get("unanswered")),
            "abandon_rate": safe_float(group.get("abandon_rate")),
            "avg_waiting_time": safe_int(group.get("avg_waiting_time")),
            "max_waiting_time": safe_int(group.get("max_waiting_time")),
            "avg_call_duration": safe_int(group.get("avg_call_duration")),
            "rt_waiting_queue": safe_int(real_time.get("waiting_queue")),
            "rt_avg_waiting_time": safe_int(real_time.get("avg_waiting_time")),
            "rt_max_waiting_time": safe_int(real_time.get("max_waiting_time")),
            "rt_avg_abandonment_time": safe_int(
                real_time.get("avg_abandonment_time")
            ),
        })

    logger.info("group_stats_transformed", count=len(transformed))
    return transformed
