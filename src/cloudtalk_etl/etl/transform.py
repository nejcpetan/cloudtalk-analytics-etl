import structlog
from collections import defaultdict
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
        agent = record.get("Agent", {})

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
            "user_id": str(cdr.get("user_id")) if cdr.get("user_id") is not None else None,
            "started_at": started_at,
            "answered_at": answered_at,
            "ended_at": parse_timestamp(cdr.get("ended_at")),
            "recording_link": cdr.get("recording_link"),
            "call_status": call_status,
            "call_date": call_date,
            "contact_id": str(contact.get("id", "")) or None,
            "contact_name": contact.get("name") or None,
            "contact_company": contact.get("company") or None,
            "agent_id": str(agent.get("id")) if agent.get("id") is not None else None,
            "agent_name": agent.get("fullname") or None if agent.get("id") is not None else None,
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


# ===========================================================================
# Phase 2: Dimension transforms
# ===========================================================================

def transform_numbers(raw_numbers: list[dict]) -> list[dict]:
    """
    Transform raw /numbers/index.json records into flat dicts for numbers_dim.

    connected_to values: 0=group, 1=agent, 2=conference, 3=fax.
    source_id is the group_id when connected_to == 0.
    """
    transformed = []

    for item in raw_numbers:
        connected_to_val = item.get("connected_to")
        transformed.append({
            "id": safe_int(item.get("id")),
            "internal_name": item.get("internal_name") or None,
            "caller_id_e164": item.get("caller_id_e164") or None,
            "country_code": safe_int(item.get("country_code")) or None,
            "connected_to": int(connected_to_val) if connected_to_val is not None else None,
            "source_id": safe_int(item.get("source_id")) or None,
        })

    logger.info("numbers_transformed", count=len(transformed))
    return transformed


def transform_groups_dim(raw_groups: list[dict]) -> list[dict]:
    """Transform raw /groups/index.json records into flat dicts for groups_dim."""
    transformed = []

    for item in raw_groups:
        transformed.append({
            "id": safe_int(item.get("id")),
            "internal_name": item.get("internal_name") or None,
        })

    logger.info("groups_dim_transformed", count=len(transformed))
    return transformed


def transform_tags(raw_tags: list[dict]) -> list[dict]:
    """Transform raw /tags/index.json records into flat dicts for tags_dim."""
    transformed = []

    for item in raw_tags:
        transformed.append({
            "id": safe_int(item.get("id")),
            "name": item.get("name") or None,
        })

    logger.info("tags_transformed", count=len(transformed))
    return transformed


def build_number_lookup(numbers: list[dict], groups: list[dict]) -> dict:
    """
    Build a lookup dict mapping number_id (int) to routing info.

    Only numbers connected to a group (connected_to == 0) will have a group_id.
    Numbers connected to agents or conferences will have group_id=None.

    Returns:
        {number_id: {"group_id": int|None, "group_name": str|None, "country_code": int|None}}
    """
    group_names = {g["id"]: g["internal_name"] for g in groups}
    lookup: dict[int, dict] = {}

    for n in numbers:
        number_id = n.get("id")
        if number_id is None:
            continue

        group_id = n.get("source_id") if n.get("connected_to") == 0 else None
        lookup[number_id] = {
            "group_id": group_id,
            "group_name": group_names.get(group_id) if group_id else None,
            "country_code": n.get("country_code"),
        }

    return lookup


# ===========================================================================
# Phase 2: Fact / aggregation transforms
# ===========================================================================

def transform_call_tags(raw_calls: list[dict]) -> list[dict]:
    """
    Extract call-tag pairs from raw call records.

    Each call can have multiple tags. Returns one dict per (call_id, tag_id) pair.
    Tags are the "Tags" top-level key on each raw call record.
    """
    result = []

    for record in raw_calls:
        cdr = record.get("Cdr", {})
        call_id = safe_int(cdr.get("id"))
        if not call_id:
            continue

        tags = record.get("Tags", []) or []
        for tag in tags:
            tag_id = safe_int(tag.get("id"))
            if not tag_id:
                continue
            result.append({
                "call_id": call_id,
                "tag_id": tag_id,
                "tag_name": tag.get("name") or None,
            })

    logger.info("call_tags_transformed", count=len(result))
    return result


def transform_call_center_daily_stats(
    raw_calls: list[dict],
    number_lookup: dict,
    sync_date: date,
) -> list[dict]:
    """
    Aggregate raw call records into per-group per-day statistics.

    Uses CallNumber.id from each call to map to a group via number_lookup.
    Calls not mapped to a known group are silently skipped (e.g. direct-to-agent calls).

    Args:
        raw_calls:     Raw call records from /calls/index.json.
        number_lookup: Built by build_number_lookup(). Keyed by number_id (int).
        sync_date:     The date being synced.

    Returns:
        List of aggregated stat dicts ready for call_center_daily_stats upsert.
    """
    # {group_id: {group_name, country_code, total, answered, missed}}
    buckets: dict[int, dict] = {}

    for record in raw_calls:
        cdr = record.get("Cdr", {})
        call_number = record.get("CallNumber", {})

        number_id = safe_int(call_number.get("id")) or None
        info = number_lookup.get(number_id) if number_id else None

        if not info or not info.get("group_id"):
            continue  # not routed to a known group

        group_id = info["group_id"]

        if group_id not in buckets:
            buckets[group_id] = {
                "group_name": info.get("group_name", "Unknown"),
                "country_code": info.get("country_code"),
                "total": 0,
                "answered": 0,
                "missed": 0,
            }

        buckets[group_id]["total"] += 1
        if cdr.get("answered_at"):
            buckets[group_id]["answered"] += 1
        else:
            buckets[group_id]["missed"] += 1

    result = []
    for group_id, data in buckets.items():
        total = data["total"]
        answered = data["answered"]
        answer_rate = round(answered / total * 100, 2) if total > 0 else 0.0
        result.append({
            "sync_date": sync_date,
            "group_id": group_id,
            "group_name": data["group_name"],
            "country_code": data["country_code"],
            "total_calls": total,
            "answered_calls": answered,
            "missed_calls": data["missed"],
            "callback_calls": 0,  # Phase 2B enrichment
            "answer_rate_pct": answer_rate,
        })

    logger.info("call_center_daily_stats_transformed",
                count=len(result), sync_date=str(sync_date))
    return result


def transform_agent_daily_stats(
    raw_calls: list[dict],
    sync_date: date,
) -> list[dict]:
    """
    Aggregate raw call records into per-agent per-day statistics.

    Only calls with a user_id (i.e. answered by an agent) contribute.
    presented_calls is initialised to 0 (Phase 2B enrichment via analytics API).

    Args:
        raw_calls:  Raw call records from /calls/index.json.
        sync_date:  The date being synced.

    Returns:
        List of aggregated stat dicts ready for agent_daily_stats upsert.
    """
    # {agent_id: {agent_name, answered, talk_seconds}}
    buckets: dict[int, dict] = {}

    for record in raw_calls:
        cdr = record.get("Cdr", {})
        agent = record.get("Agent", {})

        user_id = cdr.get("user_id")
        if user_id is None:
            continue  # missed call with no answering agent

        agent_id = safe_int(user_id)
        if agent_id == 0:
            continue

        if agent_id not in buckets:
            buckets[agent_id] = {
                "agent_name": agent.get("fullname") or None,
                "answered": 0,
                "talk_seconds": 0,
            }

        if cdr.get("answered_at"):
            buckets[agent_id]["answered"] += 1

        buckets[agent_id]["talk_seconds"] += safe_int(cdr.get("talking_time"))

    result = []
    for agent_id, data in buckets.items():
        result.append({
            "sync_date": sync_date,
            "agent_id": agent_id,
            "agent_name": data["agent_name"],
            "presented_calls": 0,  # Phase 2B
            "answered_calls": data["answered"],
            "total_talk_seconds": data["talk_seconds"],
        })

    logger.info("agent_daily_stats_transformed",
                count=len(result), sync_date=str(sync_date))
    return result


def transform_call_reasons_daily(
    raw_calls: list[dict],
    number_lookup: dict,
    sync_date: date,
) -> list[dict]:
    """
    Aggregate tag usage counts per group per day.

    Crosses call tags with the group each call was routed to, producing a
    count of how many times each tag was used in each group on the given day.

    Args:
        raw_calls:     Raw call records from /calls/index.json.
        number_lookup: Built by build_number_lookup(). Keyed by number_id (int).
        sync_date:     The date being synced.

    Returns:
        List of aggregated reason dicts ready for call_reasons_daily upsert.
    """
    # {(group_id, tag_id): {group_name, tag_name, count}}
    buckets: dict[tuple, dict] = {}

    for record in raw_calls:
        call_number = record.get("CallNumber", {})

        number_id = safe_int(call_number.get("id")) or None
        info = number_lookup.get(number_id) if number_id else None

        if not info or not info.get("group_id"):
            continue

        group_id = info["group_id"]
        group_name = info.get("group_name", "Unknown")

        tags = record.get("Tags", []) or []
        for tag in tags:
            tag_id = safe_int(tag.get("id"))
            if not tag_id:
                continue

            key = (group_id, tag_id)
            if key not in buckets:
                buckets[key] = {
                    "group_name": group_name,
                    "tag_name": tag.get("name") or None,
                    "count": 0,
                }
            buckets[key]["count"] += 1

    result = []
    for (group_id, tag_id), data in buckets.items():
        result.append({
            "sync_date": sync_date,
            "group_id": group_id,
            "group_name": data["group_name"],
            "tag_id": tag_id,
            "tag_name": data["tag_name"],
            "call_count": data["count"],
        })

    logger.info("call_reasons_daily_transformed",
                count=len(result), sync_date=str(sync_date))
    return result
