import structlog
from collections import defaultdict
from datetime import date, datetime

logger = structlog.get_logger()


# ===========================================================================
# Utility helpers (reused across transforms)
# ===========================================================================

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


def format_date_eu(d: date) -> str:
    """Format a date as DD.MM.YYYY string for QlikSense output."""
    return d.strftime("%d.%m.%Y")


_KNOWN_COUNTRIES = {"SLO", "CRO"}


def parse_group_name(group_name: str) -> tuple[str, str]:
    """
    Parse a CloudTalk group name into (country, category).

    Supported formats:
    - "Category - SLO" or "Category - CRO"  → (SLO/CRO, Category)
    - "(SLO) Category" or "(CRO) Category"  → (SLO/CRO, Category)
    Falls back to ('UNKNOWN', original_name) with a warning log.
    """
    if not group_name:
        return ("UNKNOWN", group_name or "")

    # Format: "Category - COUNTRY" (country is the last segment after " - ")
    delimiter = " - "
    idx = group_name.rfind(delimiter)
    if idx != -1:
        suffix = group_name[idx + len(delimiter):].strip()
        if suffix in _KNOWN_COUNTRIES:
            return (suffix, group_name[:idx].strip())

    logger.warning("group_name_parse_failed", group_name=group_name)
    return ("UNKNOWN", group_name)


def _get_group_name_from_detail(detail: dict) -> str | None:
    """
    Extract the authoritative group name from a call detail response.

    Looks for the first QueueStep in call_steps. Falls back to
    internal_number.name if no queue step is found.
    """
    call_steps = detail.get("call_steps") or []
    for step in call_steps:
        if step.get("type") == "queue":
            name = step.get("name")
            if name:
                return name

    # Fallback: use internal_number name
    internal_number = detail.get("internal_number") or {}
    name = internal_number.get("name")
    if name:
        return name

    return None


# ===========================================================================
# Output table transforms
# ===========================================================================

def transform_call_center_groups(
    call_details: dict[int, dict],
    sync_date: date,
) -> list[dict]:
    """
    Aggregate call detail data into per-group daily statistics.

    Uses QueueStep.name as the authoritative group source. Falls back to
    internal_number.name. Calls without a resolvable group are skipped.

    Args:
        call_details: Dict mapping call_id -> detail response.
        sync_date:    The date being synced.

    Returns:
        List of aggregated dicts ready for call_center_groups upsert.
    """
    # {group_name: {total, answered, unanswered}}
    buckets: dict[str, dict] = {}

    for call_id, detail in call_details.items():
        group_name = _get_group_name_from_detail(detail)
        if not group_name:
            logger.warning("call_no_group_skipped", call_id=call_id)
            continue

        status = detail.get("status", "")

        if group_name not in buckets:
            buckets[group_name] = {"total": 0, "answered": 0, "unanswered": 0}

        buckets[group_name]["total"] += 1
        if status == "answered":
            buckets[group_name]["answered"] += 1
        else:
            buckets[group_name]["unanswered"] += 1

    date_str = format_date_eu(sync_date)
    result = []
    for group_name, data in buckets.items():
        country, category = parse_group_name(group_name)
        if country == "UNKNOWN":
            continue
        total = data["total"]
        answered = data["answered"]
        answered_pct = round(answered / total * 100, 2) if total > 0 else None
        result.append({
            "date": date_str,
            "country": country,
            "group_name": group_name,
            "category": category,
            "total_calls": total,
            "answered_calls": answered,
            "answered_pct": answered_pct,
            "unanswered_calls": data["unanswered"],
        })

    logger.info("call_center_groups_transformed", count=len(result), sync_date=str(sync_date))
    return result


def transform_agent_stats(
    call_details: dict[int, dict],
    sync_date: date,
) -> list[dict]:
    """
    Aggregate per-agent call statistics from call detail call_steps.

    For each QueueStep in call_steps, reads agent_calls to get which agents
    were presented the call, who answered, and how long they talked.

    Args:
        call_details: Dict mapping call_id -> detail response.
        sync_date:    The date being synced.

    Returns:
        List of aggregated dicts ready for agent_stats upsert.
        PK: (date, country, group_name, agent_id)
    """
    # {(group_name, agent_id): {agent_name, presented, answered, talk_sec}}
    buckets: dict[tuple, dict] = {}

    for call_id, detail in call_details.items():
        call_steps = detail.get("call_steps") or []

        for step in call_steps:
            if step.get("type") != "queue":
                continue

            group_name = step.get("name") or ""
            if not group_name:
                continue

            agent_calls = step.get("agent_calls") or []
            for agent_step in agent_calls:
                agent_id = safe_int(agent_step.get("id"))
                if not agent_id:
                    continue

                key = (group_name, agent_id)
                if key not in buckets:
                    buckets[key] = {
                        "agent_name": agent_step.get("name") or None,
                        "presented": 0,
                        "answered": 0,
                        "talk_sec": 0,
                    }

                buckets[key]["presented"] += 1

                if agent_step.get("status") == "answered":
                    buckets[key]["answered"] += 1
                    call_times = agent_step.get("call_times") or {}
                    buckets[key]["talk_sec"] += safe_int(call_times.get("talking_time"))

    date_str = format_date_eu(sync_date)
    result = []
    for (group_name, agent_id), data in buckets.items():
        country, category = parse_group_name(group_name)
        if country == "UNKNOWN":
            continue
        result.append({
            "date": date_str,
            "country": country,
            "group_name": group_name,
            "category": category,
            "agent_id": agent_id,
            "agent_name": data["agent_name"],
            "presented_calls": data["presented"],
            "answered_calls": data["answered"],
            "talking_time_sec": data["talk_sec"],
        })

    logger.info("agent_stats_transformed", count=len(result), sync_date=str(sync_date))
    return result


def transform_call_reasons(
    call_details: dict[int, dict],
    sync_date: date,
) -> list[dict]:
    """
    Aggregate tag (call reason) usage counts per group per day.

    For each call, reads call_tags and associates them with the group
    from the first QueueStep. Calls without a group or tags are skipped.

    Args:
        call_details: Dict mapping call_id -> detail response.
        sync_date:    The date being synced.

    Returns:
        List of aggregated dicts ready for call_reasons upsert.
        PK: (date, country, group_name, tag_id)
    """
    # {(group_name, tag_id): {tag_name, count}}
    buckets: dict[tuple, dict] = {}

    for call_id, detail in call_details.items():
        group_name = _get_group_name_from_detail(detail)
        if not group_name:
            continue

        call_tags = detail.get("call_tags") or []
        for tag in call_tags:
            tag_id = safe_int(tag.get("id"))
            if not tag_id:
                continue

            key = (group_name, tag_id)
            if key not in buckets:
                # The detail response uses "label" not "name" for tag text
                buckets[key] = {
                    "tag_name": tag.get("label") or None,
                    "count": 0,
                }
            buckets[key]["count"] += 1

    date_str = format_date_eu(sync_date)
    result = []
    for (group_name, tag_id), data in buckets.items():
        country, category = parse_group_name(group_name)
        if country == "UNKNOWN":
            continue
        result.append({
            "date": date_str,
            "country": country,
            "group_name": group_name,
            "category": category,
            "tag_id": tag_id,
            "tag_name": data["tag_name"],
            "call_count": data["count"],
        })

    logger.info("call_reasons_transformed", count=len(result), sync_date=str(sync_date))
    return result
