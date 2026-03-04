import sys
import time
import structlog
from datetime import date, timedelta

from cloudtalk_etl.config import Settings
from cloudtalk_etl.api.client import CloudTalkClient
from cloudtalk_etl.api.rate_limiter import TokenBucketRateLimiter
from cloudtalk_etl.db.connection import get_connection
from cloudtalk_etl.db.schema import ensure_schema
from cloudtalk_etl.etl.extract import extract_calls, extract_agents, extract_group_stats
from cloudtalk_etl.etl.transform import transform_calls, transform_agents, transform_group_stats
from cloudtalk_etl.etl.load import load_calls, load_agents, load_group_stats

logger = structlog.get_logger()


def determine_sync_date(override: str | None) -> date:
    """Determine which date to sync. Default: yesterday."""
    if override:
        return date.fromisoformat(override)
    return date.today() - timedelta(days=1)


def run_etl() -> None:
    """Main ETL entry point with graceful degradation across pipeline stages."""
    start_time = time.monotonic()
    settings = Settings()
    sync_date = determine_sync_date(settings.etl_date_override)

    logger.info("etl_started", sync_date=str(sync_date), test_mode=settings.test_mode)

    rate_limiter = TokenBucketRateLimiter(rate_per_minute=settings.rate_limit_rpm)
    api_client = CloudTalkClient(
        api_key_id=settings.cloudtalk_api_key_id,
        api_key_secret=settings.cloudtalk_api_key_secret,
        base_url=settings.cloudtalk_api_base_url,
        rate_limiter=rate_limiter,
    )
    conn = get_connection(settings.database_url)

    calls_count = 0
    agents_count = 0
    groups_count = 0
    failed_stages = []

    try:
        ensure_schema(conn)

        # === CALLS ===
        try:
            raw_calls = extract_calls(api_client, sync_date, test_mode=settings.test_mode)
            calls = transform_calls(raw_calls, sync_date)
            calls_count = load_calls(conn, calls)
        except Exception:
            logger.exception("stage_failed", stage="calls", sync_date=str(sync_date))
            failed_stages.append("calls")

        # === AGENTS ===
        try:
            raw_agents = extract_agents(api_client, test_mode=settings.test_mode)
            agents = transform_agents(raw_agents, sync_date)
            agents_count = load_agents(conn, agents, sync_date)
        except Exception:
            logger.exception("stage_failed", stage="agents", sync_date=str(sync_date))
            failed_stages.append("agents")

        # === GROUP STATS ===
        try:
            raw_group_stats = extract_group_stats(api_client)
            group_stats = transform_group_stats(raw_group_stats, sync_date)
            groups_count = load_group_stats(conn, group_stats, sync_date)
        except Exception:
            logger.exception("stage_failed", stage="group_stats", sync_date=str(sync_date))
            failed_stages.append("group_stats")

    except Exception:
        elapsed = time.monotonic() - start_time
        logger.exception("etl_failed", sync_date=str(sync_date),
                         duration_seconds=round(elapsed, 2))
        sys.exit(1)
    finally:
        api_client.close()
        conn.close()

    elapsed = time.monotonic() - start_time

    if failed_stages:
        logger.error(
            "etl_completed_with_errors",
            sync_date=str(sync_date),
            failed_stages=failed_stages,
            calls_synced=calls_count,
            agents_synced=agents_count,
            groups_synced=groups_count,
            duration_seconds=round(elapsed, 2),
        )
        sys.exit(1)

    logger.info(
        "etl_completed",
        sync_date=str(sync_date),
        calls_synced=calls_count,
        agents_synced=agents_count,
        groups_synced=groups_count,
        duration_seconds=round(elapsed, 2),
    )
