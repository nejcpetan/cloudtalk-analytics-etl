import sys
import time
import structlog
from datetime import date, timedelta

from cloudtalk_etl.config import Settings
from cloudtalk_etl.api.client import CloudTalkClient
from cloudtalk_etl.api.rate_limiter import TokenBucketRateLimiter
from cloudtalk_etl.db.connection import get_connection
from cloudtalk_etl.db.schema import ensure_schema
from cloudtalk_etl.etl.extract import (
    extract_calls,
    extract_agents,
    extract_group_stats,
    extract_groups_dim,
    extract_numbers,
    extract_tags,
)
from cloudtalk_etl.etl.transform import (
    transform_calls,
    transform_agents,
    transform_group_stats,
    transform_numbers,
    transform_groups_dim,
    transform_tags,
    build_number_lookup,
    transform_call_tags,
    transform_call_center_daily_stats,
    transform_agent_daily_stats,
    transform_call_reasons_daily,
)
from cloudtalk_etl.etl.load import (
    load_calls,
    load_agents,
    load_group_stats,
    load_numbers_dim,
    load_groups_dim,
    load_tags_dim,
    load_call_tags,
    load_call_center_daily_stats,
    load_agent_daily_stats,
    load_call_reasons_daily,
)

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

    # Counters — phase 1
    calls_count = 0
    agents_count = 0
    groups_count = 0

    # Counters — phase 2
    numbers_dim_count = 0
    groups_dim_count = 0
    tags_dim_count = 0
    call_tags_count = 0
    cc_stats_count = 0
    agent_stats_count = 0
    reasons_count = 0

    failed_stages: list[str] = []

    # These are shared across stages — initialise before any try block
    raw_calls: list[dict] = []
    numbers: list[dict] = []
    groups_dim_list: list[dict] = []

    try:
        ensure_schema(conn)

        # =====================================================================
        # PHASE 1 — Core data
        # =====================================================================

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

        # === GROUP STATS (realtime snapshot) ===
        try:
            raw_group_stats = extract_group_stats(api_client)
            group_stats = transform_group_stats(raw_group_stats, sync_date)
            groups_count = load_group_stats(conn, group_stats, sync_date)
        except Exception:
            logger.exception("stage_failed", stage="group_stats", sync_date=str(sync_date))
            failed_stages.append("group_stats")

        # =====================================================================
        # PHASE 2 — Dimensions
        # =====================================================================

        # === NUMBERS DIM ===
        try:
            raw_numbers = extract_numbers(api_client)
            numbers = transform_numbers(raw_numbers)
            numbers_dim_count = load_numbers_dim(conn, numbers)
        except Exception:
            logger.exception("stage_failed", stage="numbers_dim", sync_date=str(sync_date))
            failed_stages.append("numbers_dim")

        # === GROUPS DIM ===
        try:
            raw_groups_dim = extract_groups_dim(api_client)
            groups_dim_list = transform_groups_dim(raw_groups_dim)
            groups_dim_count = load_groups_dim(conn, groups_dim_list)
        except Exception:
            logger.exception("stage_failed", stage="groups_dim", sync_date=str(sync_date))
            failed_stages.append("groups_dim")

        # === TAGS DIM ===
        try:
            raw_tags = extract_tags(api_client)
            tags = transform_tags(raw_tags)
            tags_dim_count = load_tags_dim(conn, tags)
        except Exception:
            logger.exception("stage_failed", stage="tags_dim", sync_date=str(sync_date))
            failed_stages.append("tags_dim")

        # =====================================================================
        # PHASE 2 — Analytics (all derived from raw_calls — skip if calls failed)
        # =====================================================================

        if "calls" not in failed_stages:
            number_lookup = build_number_lookup(numbers, groups_dim_list)

            # === CALL TAGS ===
            try:
                call_tag_pairs = transform_call_tags(raw_calls)
                call_tags_count = load_call_tags(conn, call_tag_pairs)
            except Exception:
                logger.exception("stage_failed", stage="call_tags", sync_date=str(sync_date))
                failed_stages.append("call_tags")

            # === CALL CENTER DAILY STATS ===
            try:
                cc_stats = transform_call_center_daily_stats(
                    raw_calls, number_lookup, sync_date
                )
                cc_stats_count = load_call_center_daily_stats(conn, cc_stats, sync_date)
            except Exception:
                logger.exception("stage_failed", stage="call_center_daily_stats",
                                 sync_date=str(sync_date))
                failed_stages.append("call_center_daily_stats")

            # === AGENT DAILY STATS ===
            try:
                agent_stats = transform_agent_daily_stats(raw_calls, sync_date)
                agent_stats_count = load_agent_daily_stats(conn, agent_stats, sync_date)
            except Exception:
                logger.exception("stage_failed", stage="agent_daily_stats",
                                 sync_date=str(sync_date))
                failed_stages.append("agent_daily_stats")

            # === CALL REASONS DAILY ===
            try:
                reasons = transform_call_reasons_daily(raw_calls, number_lookup, sync_date)
                reasons_count = load_call_reasons_daily(conn, reasons, sync_date)
            except Exception:
                logger.exception("stage_failed", stage="call_reasons_daily",
                                 sync_date=str(sync_date))
                failed_stages.append("call_reasons_daily")

        else:
            logger.warning("analytics_stages_skipped",
                           reason="calls stage failed",
                           sync_date=str(sync_date))

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
            numbers_dim_synced=numbers_dim_count,
            groups_dim_synced=groups_dim_count,
            tags_dim_synced=tags_dim_count,
            call_tags_synced=call_tags_count,
            call_center_stats_synced=cc_stats_count,
            agent_stats_synced=agent_stats_count,
            call_reasons_synced=reasons_count,
            duration_seconds=round(elapsed, 2),
        )
        sys.exit(1)

    logger.info(
        "etl_completed",
        sync_date=str(sync_date),
        calls_synced=calls_count,
        agents_synced=agents_count,
        groups_synced=groups_count,
        numbers_dim_synced=numbers_dim_count,
        groups_dim_synced=groups_dim_count,
        tags_dim_synced=tags_dim_count,
        call_tags_synced=call_tags_count,
        call_center_stats_synced=cc_stats_count,
        agent_stats_synced=agent_stats_count,
        call_reasons_synced=reasons_count,
        duration_seconds=round(elapsed, 2),
    )
