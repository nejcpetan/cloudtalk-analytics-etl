import sys
import time
import structlog
from datetime import date, timedelta

from cloudtalk_etl.config import Settings
from cloudtalk_etl.api.client import CloudTalkClient
from cloudtalk_etl.api.rate_limiter import TokenBucketRateLimiter
from cloudtalk_etl.db.backend import get_connection, ensure_schema
from cloudtalk_etl.etl.extract import (
    extract_calls,
    extract_call_details,
    extract_groups,
    extract_tags,
)
from cloudtalk_etl.etl.transform import (
    transform_call_center_groups,
    transform_agent_stats,
    transform_call_reasons,
)
from cloudtalk_etl.etl.load import (
    load_call_center_groups,
    load_agent_stats,
    load_call_reasons,
)

logger = structlog.get_logger()


def determine_sync_date(override: str | None) -> date:
    """Determine which date to sync. Default: yesterday."""
    if override:
        return date.fromisoformat(override)
    return date.today() - timedelta(days=1)


def run_etl() -> None:
    """Main ETL entry point."""
    start_time = time.monotonic()
    settings = Settings()
    sync_date = determine_sync_date(settings.etl_date_override)

    logger.info("etl_started", sync_date=str(sync_date), test_mode=settings.test_mode)

    rate_limiter = TokenBucketRateLimiter(rate_per_minute=settings.rate_limit_rpm)
    api_client = CloudTalkClient(
        api_key_id=settings.cloudtalk_api_key_id,
        api_key_secret=settings.cloudtalk_api_key_secret,
        base_url=settings.cloudtalk_api_base_url,
        analytics_base_url=settings.cloudtalk_analytics_api_base_url,
        rate_limiter=rate_limiter,
    )

    cc_groups_count = 0
    agent_stats_count = 0
    call_reasons_count = 0
    failed_stages: list[str] = []

    try:
        # =====================================================================
        # Step 0: Ensure schema — short-lived connection, closed immediately.
        # The API extraction below takes ~20 min, which would exhaust any idle
        # connection kept open throughout.
        # =====================================================================
        conn = get_connection(settings.database_url)
        try:
            ensure_schema(conn)
        finally:
            conn.close()

        # =====================================================================
        # Step 1: Fetch calls index for target date
        # =====================================================================
        raw_calls: list[dict] = []
        try:
            raw_calls = extract_calls(api_client, sync_date, test_mode=settings.test_mode)
        except Exception:
            logger.exception("stage_failed", stage="calls_index", sync_date=str(sync_date))
            failed_stages.append("calls_index")

        # =====================================================================
        # Step 2: Fetch call details (throttled — 1050ms between requests)
        # Required for authoritative group assignment and agent step data.
        # =====================================================================
        call_details: dict[int, dict] = {}
        if "calls_index" not in failed_stages and raw_calls:
            try:
                call_details = extract_call_details(
                    api_client, raw_calls,
                    test_mode=settings.test_mode,
                    sample_size=settings.test_sample_size,
                )
            except Exception:
                logger.exception("stage_failed", stage="call_details", sync_date=str(sync_date))
                failed_stages.append("call_details")

        # =====================================================================
        # Step 3: Transform all 3 tables in memory, then open a fresh DB
        # connection for the load — avoids idle timeout from the long extract.
        # =====================================================================
        if "call_details" not in failed_stages and call_details:
            conn = get_connection(settings.database_url)
            try:
                # --- Table 1: call_center_groups ---
                try:
                    cc_groups = transform_call_center_groups(call_details, sync_date)
                    cc_groups_count = load_call_center_groups(conn, cc_groups)
                except Exception:
                    conn.rollback()
                    logger.exception("stage_failed", stage="call_center_groups",
                                     sync_date=str(sync_date))
                    failed_stages.append("call_center_groups")

                # --- Table 2: agent_stats ---
                try:
                    agent_rows = transform_agent_stats(call_details, sync_date)
                    agent_stats_count = load_agent_stats(conn, agent_rows)
                except Exception:
                    conn.rollback()
                    logger.exception("stage_failed", stage="agent_stats",
                                     sync_date=str(sync_date))
                    failed_stages.append("agent_stats")

                # --- Table 3: call_reasons ---
                try:
                    reasons = transform_call_reasons(call_details, sync_date)
                    call_reasons_count = load_call_reasons(conn, reasons)
                except Exception:
                    conn.rollback()
                    logger.exception("stage_failed", stage="call_reasons",
                                     sync_date=str(sync_date))
                    failed_stages.append("call_reasons")
            finally:
                conn.close()

        elif not raw_calls:
            logger.warning("no_calls_for_date", sync_date=str(sync_date))
        elif not call_details:
            logger.warning("all_call_details_skipped", sync_date=str(sync_date))

    except Exception:
        elapsed = time.monotonic() - start_time
        logger.exception("etl_failed", sync_date=str(sync_date),
                         duration_seconds=round(elapsed, 2))
        sys.exit(1)
    finally:
        api_client.close()

    elapsed = time.monotonic() - start_time

    if failed_stages:
        logger.error(
            "etl_completed_with_errors",
            sync_date=str(sync_date),
            failed_stages=failed_stages,
            call_center_groups_synced=cc_groups_count,
            agent_stats_synced=agent_stats_count,
            call_reasons_synced=call_reasons_count,
            duration_seconds=round(elapsed, 2),
        )
        sys.exit(1)

    logger.info(
        "etl_completed",
        sync_date=str(sync_date),
        calls_in_index=len(raw_calls),
        call_details_fetched=len(call_details),
        call_center_groups_synced=cc_groups_count,
        agent_stats_synced=agent_stats_count,
        call_reasons_synced=call_reasons_count,
        duration_seconds=round(elapsed, 2),
    )
