import structlog

logger = structlog.get_logger()

# MySQL uses ON DUPLICATE KEY UPDATE (not ON CONFLICT).
# Parameters use positional %s — mysql-connector-python is more
# reliably tested with positional params than named %(key)s in executemany.


def upsert_call_center_groups(conn, rows: list[dict]) -> int:
    """
    Upsert rows into call_center_groups.

    ON DUPLICATE KEY (date, country, group_name) → update all metric columns.
    """
    if not rows:
        return 0

    sql = """
        INSERT INTO call_center_groups
            (date, country, group_name, category,
             total_calls, answered_calls, answered_pct, unanswered_calls)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            category         = VALUES(category),
            total_calls      = VALUES(total_calls),
            answered_calls   = VALUES(answered_calls),
            answered_pct     = VALUES(answered_pct),
            unanswered_calls = VALUES(unanswered_calls),
            synced_at        = NOW()
    """
    params = [
        (r["date"], r["country"], r["group_name"], r["category"],
         r["total_calls"], r["answered_calls"], r["answered_pct"], r["unanswered_calls"])
        for r in rows
    ]
    with conn.cursor() as cur:
        cur.executemany(sql, params)
    conn.commit()

    logger.info("upserted_call_center_groups", count=len(rows), backend="mysql")
    return len(rows)


def upsert_agent_stats(conn, rows: list[dict]) -> int:
    """
    Upsert rows into agent_stats.

    ON DUPLICATE KEY (date, country, group_name, agent_id) → update all metric columns.
    """
    if not rows:
        return 0

    sql = """
        INSERT INTO agent_stats
            (date, country, group_name, category,
             agent_id, agent_name, presented_calls, answered_calls, talking_time_sec)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            category         = VALUES(category),
            agent_name       = VALUES(agent_name),
            presented_calls  = VALUES(presented_calls),
            answered_calls   = VALUES(answered_calls),
            talking_time_sec = VALUES(talking_time_sec),
            synced_at        = NOW()
    """
    params = [
        (r["date"], r["country"], r["group_name"], r["category"],
         r["agent_id"], r["agent_name"], r["presented_calls"],
         r["answered_calls"], r["talking_time_sec"])
        for r in rows
    ]
    with conn.cursor() as cur:
        cur.executemany(sql, params)
    conn.commit()

    logger.info("upserted_agent_stats", count=len(rows), backend="mysql")
    return len(rows)


def upsert_call_reasons(conn, rows: list[dict]) -> int:
    """
    Upsert rows into call_reasons.

    ON DUPLICATE KEY (date, country, group_name, tag_id) → update call_count.
    """
    if not rows:
        return 0

    sql = """
        INSERT INTO call_reasons
            (date, country, group_name, category, tag_id, tag_name, call_count)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            category   = VALUES(category),
            tag_name   = VALUES(tag_name),
            call_count = VALUES(call_count),
            synced_at  = NOW()
    """
    params = [
        (r["date"], r["country"], r["group_name"], r["category"],
         r["tag_id"], r["tag_name"], r["call_count"])
        for r in rows
    ]
    with conn.cursor() as cur:
        cur.executemany(sql, params)
    conn.commit()

    logger.info("upserted_call_reasons", count=len(rows), backend="mysql")
    return len(rows)
