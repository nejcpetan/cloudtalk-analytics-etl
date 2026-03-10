import random
import time
import structlog
from datetime import date

from cloudtalk_etl.api.client import CloudTalkClient

logger = structlog.get_logger()

# Minimum delay between /calls/{callId} detail requests to stay within 60 req/min
_DETAIL_THROTTLE_SECONDS = 1.05


def extract_calls(client: CloudTalkClient, sync_date: date,
                  test_mode: bool = False) -> list[dict]:
    """
    Extract all calls for a given date from the index endpoint.

    Uses date_from (start of day) and date_to (end of day) filters.
    Handles pagination automatically. Always fetches the full index —
    sampling is applied downstream in extract_call_details.
    """
    date_from = f"{sync_date} 00:00:00"
    date_to = f"{sync_date} 23:59:59"

    logger.info("extracting_calls", date_from=date_from, date_to=date_to,
                test_mode=test_mode)

    calls = client.get_all_pages(
        client.get_calls,
        date_from=date_from,
        date_to=date_to,
        limit=1000,
    )

    logger.info("calls_extracted", count=len(calls))
    return calls


def extract_call_details(client: CloudTalkClient, raw_calls: list[dict],
                         test_mode: bool = False,
                         sample_size: int = 50) -> dict[int, dict]:
    """
    Fetch detailed call data for each call from the analytics API.

    Calls GET /calls/{callId} for each call in raw_calls, throttled at
    1050ms between requests to stay within the 60 req/min API limit.
    Individual failures are logged and skipped — the returned dict will
    simply be missing that call_id.

    Args:
        client:      CloudTalk API client.
        raw_calls:   Raw call index records (each has a Cdr.id field).
        test_mode:   If True, fetch only a random sample of call details.
        sample_size: Number of calls to sample in test_mode (default 50).

    Returns:
        Dict mapping call_id (int) -> detail response dict.
    """
    call_ids = []
    for record in raw_calls:
        cdr = record.get("Cdr", {})
        call_id = cdr.get("id")
        if call_id is not None:
            try:
                call_ids.append(int(call_id))
            except (ValueError, TypeError):
                pass

    if test_mode:
        call_ids = random.sample(call_ids, min(sample_size, len(call_ids)))

    logger.info("extracting_call_details", total=len(call_ids))

    details: dict[int, dict] = {}
    for i, call_id in enumerate(call_ids):
        if i > 0:
            time.sleep(_DETAIL_THROTTLE_SECONDS)

        try:
            detail = client.get_call_detail(call_id)
            details[call_id] = detail
        except Exception:
            logger.warning("call_detail_fetch_failed", call_id=call_id)

    logger.info("call_details_extracted", fetched=len(details), skipped=len(call_ids) - len(details))
    return details


def extract_groups(client: CloudTalkClient) -> list[dict]:
    """Extract all call center groups."""
    logger.info("extracting_groups")

    groups = client.get_all_pages(client.get_groups, limit=1000)

    logger.info("groups_extracted", count=len(groups))
    return groups


def extract_tags(client: CloudTalkClient) -> list[dict]:
    """Extract all call reason tags."""
    logger.info("extracting_tags")

    tags = client.get_all_pages(client.get_tags, limit=1000)

    logger.info("tags_extracted", count=len(tags))
    return tags
