import structlog
from datetime import date

from cloudtalk_etl.api.client import CloudTalkClient

logger = structlog.get_logger()


def extract_calls(client: CloudTalkClient, sync_date: date,
                  test_mode: bool = False) -> list[dict]:
    """
    Extract all calls for a given date.

    Uses date_from (start of day) and date_to (end of day) filters.
    Handles pagination automatically.
    """
    date_from = f"{sync_date} 00:00:00"
    date_to = f"{sync_date} 23:59:59"
    limit = 10 if test_mode else 1000

    logger.info("extracting_calls", date_from=date_from, date_to=date_to,
                test_mode=test_mode)

    calls = client.get_all_pages(
        client.get_calls,
        max_pages=1 if test_mode else None,
        date_from=date_from,
        date_to=date_to,
        limit=limit,
    )

    logger.info("calls_extracted", count=len(calls))
    return calls


def extract_agents(client: CloudTalkClient, test_mode: bool = False) -> list[dict]:
    """Extract all agents."""
    logger.info("extracting_agents", test_mode=test_mode)

    agents = client.get_all_pages(
        client.get_agents,
        max_pages=1 if test_mode else None,
        limit=10 if test_mode else 1000,
    )

    logger.info("agents_extracted", count=len(agents))
    return agents


def extract_group_stats(client: CloudTalkClient) -> list[dict]:
    """Extract group statistics snapshot."""
    logger.info("extracting_group_stats")

    response = client.get_group_stats()
    groups = response.get("responseData", {}).get("data", {}).get("groups", [])

    logger.info("group_stats_extracted", count=len(groups))
    return groups


def extract_groups_dim(client: CloudTalkClient) -> list[dict]:
    """Extract all groups for the groups dimension table."""
    logger.info("extracting_groups_dim")

    groups = client.get_all_pages(client.get_groups, limit=1000)

    logger.info("groups_dim_extracted", count=len(groups))
    return groups


def extract_numbers(client: CloudTalkClient) -> list[dict]:
    """Extract all phone numbers with their routing configuration."""
    logger.info("extracting_numbers")

    numbers = client.get_all_pages(client.get_numbers, limit=1000)

    logger.info("numbers_extracted", count=len(numbers))
    return numbers


def extract_tags(client: CloudTalkClient) -> list[dict]:
    """Extract all call reason tags."""
    logger.info("extracting_tags")

    tags = client.get_all_pages(client.get_tags, limit=1000)

    logger.info("tags_extracted", count=len(tags))
    return tags
