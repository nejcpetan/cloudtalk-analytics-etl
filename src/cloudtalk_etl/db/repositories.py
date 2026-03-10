import psycopg
import structlog

logger = structlog.get_logger()


def upsert_call_center_groups(conn: psycopg.Connection, rows: list[dict]) -> int:
    """
    Upsert rows into call_center_groups.

    ON CONFLICT (date, country, group_name) → update all metric columns.

    Args:
        conn: Active psycopg connection (autocommit=False).
        rows: List of dicts with keys: date, country, group_name, category,
              total_calls, answered_calls, answered_pct, unanswered_calls.

    Returns:
        Number of rows upserted.
    """
    if not rows:
        return 0

    sql = """
        INSERT INTO call_center_groups
            (date, country, group_name, category,
             total_calls, answered_calls, answered_pct, unanswered_calls)
        VALUES
            (%(date)s, %(country)s, %(group_name)s, %(category)s,
             %(total_calls)s, %(answered_calls)s, %(answered_pct)s, %(unanswered_calls)s)
        ON CONFLICT (date, country, group_name) DO UPDATE SET
            category         = EXCLUDED.category,
            total_calls      = EXCLUDED.total_calls,
            answered_calls   = EXCLUDED.answered_calls,
            answered_pct     = EXCLUDED.answered_pct,
            unanswered_calls = EXCLUDED.unanswered_calls,
            synced_at        = NOW()
    """
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    conn.commit()

    logger.info("upserted_call_center_groups", count=len(rows))
    return len(rows)


def upsert_agent_stats(conn: psycopg.Connection, rows: list[dict]) -> int:
    """
    Upsert rows into agent_stats.

    ON CONFLICT (date, country, group_name, agent_id) → update all metric columns.

    Args:
        conn: Active psycopg connection (autocommit=False).
        rows: List of dicts with keys: date, country, group_name, category,
              agent_id, agent_name, presented_calls, answered_calls, talking_time_sec.

    Returns:
        Number of rows upserted.
    """
    if not rows:
        return 0

    sql = """
        INSERT INTO agent_stats
            (date, country, group_name, category,
             agent_id, agent_name, presented_calls, answered_calls, talking_time_sec)
        VALUES
            (%(date)s, %(country)s, %(group_name)s, %(category)s,
             %(agent_id)s, %(agent_name)s, %(presented_calls)s, %(answered_calls)s, %(talking_time_sec)s)
        ON CONFLICT (date, country, group_name, agent_id) DO UPDATE SET
            category         = EXCLUDED.category,
            agent_name       = EXCLUDED.agent_name,
            presented_calls  = EXCLUDED.presented_calls,
            answered_calls   = EXCLUDED.answered_calls,
            talking_time_sec = EXCLUDED.talking_time_sec,
            synced_at        = NOW()
    """
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    conn.commit()

    logger.info("upserted_agent_stats", count=len(rows))
    return len(rows)


def upsert_call_reasons(conn: psycopg.Connection, rows: list[dict]) -> int:
    """
    Upsert rows into call_reasons.

    ON CONFLICT (date, country, group_name, tag_id) → update call_count.

    Args:
        conn: Active psycopg connection (autocommit=False).
        rows: List of dicts with keys: date, country, group_name, category,
              tag_id, tag_name, call_count.

    Returns:
        Number of rows upserted.
    """
    if not rows:
        return 0

    sql = """
        INSERT INTO call_reasons
            (date, country, group_name, category, tag_id, tag_name, call_count)
        VALUES
            (%(date)s, %(country)s, %(group_name)s, %(category)s,
             %(tag_id)s, %(tag_name)s, %(call_count)s)
        ON CONFLICT (date, country, group_name, tag_id) DO UPDATE SET
            category   = EXCLUDED.category,
            tag_name   = EXCLUDED.tag_name,
            call_count = EXCLUDED.call_count,
            synced_at  = NOW()
    """
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    conn.commit()

    logger.info("upserted_call_reasons", count=len(rows))
    return len(rows)
