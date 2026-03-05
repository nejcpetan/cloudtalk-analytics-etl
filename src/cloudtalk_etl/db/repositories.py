import psycopg
import structlog
from datetime import date

logger = structlog.get_logger()


def upsert_calls(conn: psycopg.Connection, calls: list[dict]) -> int:
    """
    Batch upsert call records.

    Uses PostgreSQL ON CONFLICT to handle idempotent re-runs.
    Returns the number of rows upserted.
    """
    if not calls:
        return 0

    query = """
        INSERT INTO calls (
            id, call_type, billsec, talking_time, waiting_time, wrapup_time,
            public_external, public_internal, country_code, recorded,
            is_voicemail, is_redirected, redirected_from, user_id,
            started_at, answered_at, ended_at, recording_link,
            call_status, call_date,
            contact_id, contact_name, contact_company,
            agent_id, agent_name, synced_at
        ) VALUES (
            %(id)s, %(call_type)s, %(billsec)s, %(talking_time)s,
            %(waiting_time)s, %(wrapup_time)s, %(public_external)s,
            %(public_internal)s, %(country_code)s, %(recorded)s,
            %(is_voicemail)s, %(is_redirected)s, %(redirected_from)s,
            %(user_id)s, %(started_at)s, %(answered_at)s, %(ended_at)s,
            %(recording_link)s, %(call_status)s, %(call_date)s,
            %(contact_id)s, %(contact_name)s, %(contact_company)s,
            %(agent_id)s, %(agent_name)s, NOW()
        )
        ON CONFLICT (id) DO UPDATE SET
            call_type = EXCLUDED.call_type,
            billsec = EXCLUDED.billsec,
            talking_time = EXCLUDED.talking_time,
            waiting_time = EXCLUDED.waiting_time,
            wrapup_time = EXCLUDED.wrapup_time,
            public_external = EXCLUDED.public_external,
            public_internal = EXCLUDED.public_internal,
            recorded = EXCLUDED.recorded,
            answered_at = EXCLUDED.answered_at,
            ended_at = EXCLUDED.ended_at,
            call_status = EXCLUDED.call_status,
            contact_id = EXCLUDED.contact_id,
            contact_name = EXCLUDED.contact_name,
            contact_company = EXCLUDED.contact_company,
            agent_id = EXCLUDED.agent_id,
            agent_name = EXCLUDED.agent_name,
            synced_at = NOW()
    """

    with conn.cursor() as cur:
        cur.executemany(query, calls)

    conn.commit()
    count = len(calls)
    logger.info("calls_upserted", count=count)
    return count


def upsert_agents(conn: psycopg.Connection, agents: list[dict],
                  sync_date: date) -> int:
    """Batch upsert agent snapshots for a given date."""
    if not agents:
        return 0

    query = """
        INSERT INTO agents (
            id, sync_date, firstname, lastname, fullname, email,
            availability_status, extension, default_number,
            associated_numbers, synced_at
        ) VALUES (
            %(id)s, %(sync_date)s, %(firstname)s, %(lastname)s,
            %(fullname)s, %(email)s, %(availability_status)s,
            %(extension)s, %(default_number)s,
            %(associated_numbers)s, NOW()
        )
        ON CONFLICT (id, sync_date) DO UPDATE SET
            firstname = EXCLUDED.firstname,
            lastname = EXCLUDED.lastname,
            fullname = EXCLUDED.fullname,
            email = EXCLUDED.email,
            availability_status = EXCLUDED.availability_status,
            extension = EXCLUDED.extension,
            default_number = EXCLUDED.default_number,
            associated_numbers = EXCLUDED.associated_numbers,
            synced_at = NOW()
    """

    with conn.cursor() as cur:
        cur.executemany(query, agents)

    conn.commit()
    count = len(agents)
    logger.info("agents_upserted", count=count, sync_date=str(sync_date))
    return count


def upsert_group_stats(conn: psycopg.Connection, stats: list[dict],
                       sync_date: date) -> int:
    """Batch upsert group statistics for a given date."""
    if not stats:
        return 0

    query = """
        INSERT INTO group_stats_daily (
            group_id, group_name, sync_date, operators, answered,
            unanswered, abandon_rate, avg_waiting_time, max_waiting_time,
            avg_call_duration, rt_waiting_queue, rt_avg_waiting_time,
            rt_max_waiting_time, rt_avg_abandonment_time, synced_at
        ) VALUES (
            %(group_id)s, %(group_name)s, %(sync_date)s, %(operators)s,
            %(answered)s, %(unanswered)s, %(abandon_rate)s,
            %(avg_waiting_time)s, %(max_waiting_time)s,
            %(avg_call_duration)s, %(rt_waiting_queue)s,
            %(rt_avg_waiting_time)s, %(rt_max_waiting_time)s,
            %(rt_avg_abandonment_time)s, NOW()
        )
        ON CONFLICT (group_id, sync_date) DO UPDATE SET
            group_name = EXCLUDED.group_name,
            operators = EXCLUDED.operators,
            answered = EXCLUDED.answered,
            unanswered = EXCLUDED.unanswered,
            abandon_rate = EXCLUDED.abandon_rate,
            avg_waiting_time = EXCLUDED.avg_waiting_time,
            max_waiting_time = EXCLUDED.max_waiting_time,
            avg_call_duration = EXCLUDED.avg_call_duration,
            rt_waiting_queue = EXCLUDED.rt_waiting_queue,
            rt_avg_waiting_time = EXCLUDED.rt_avg_waiting_time,
            rt_max_waiting_time = EXCLUDED.rt_max_waiting_time,
            rt_avg_abandonment_time = EXCLUDED.rt_avg_abandonment_time,
            synced_at = NOW()
    """

    with conn.cursor() as cur:
        cur.executemany(query, stats)

    conn.commit()
    count = len(stats)
    logger.info("group_stats_upserted", count=count, sync_date=str(sync_date))
    return count


# ===========================================================================
# Phase 2: Dimension table upserts
# ===========================================================================

def upsert_numbers_dim(conn: psycopg.Connection, numbers: list[dict]) -> int:
    """Upsert phone number dimension records."""
    if not numbers:
        return 0

    query = """
        INSERT INTO numbers_dim (
            id, internal_name, caller_id_e164, country_code,
            connected_to, source_id, synced_at
        ) VALUES (
            %(id)s, %(internal_name)s, %(caller_id_e164)s, %(country_code)s,
            %(connected_to)s, %(source_id)s, NOW()
        )
        ON CONFLICT (id) DO UPDATE SET
            internal_name  = EXCLUDED.internal_name,
            caller_id_e164 = EXCLUDED.caller_id_e164,
            country_code   = EXCLUDED.country_code,
            connected_to   = EXCLUDED.connected_to,
            source_id      = EXCLUDED.source_id,
            synced_at      = NOW()
    """

    with conn.cursor() as cur:
        cur.executemany(query, numbers)

    conn.commit()
    count = len(numbers)
    logger.info("numbers_dim_upserted", count=count)
    return count


def upsert_groups_dim(conn: psycopg.Connection, groups: list[dict]) -> int:
    """Upsert group dimension records."""
    if not groups:
        return 0

    query = """
        INSERT INTO groups_dim (id, internal_name, synced_at)
        VALUES (%(id)s, %(internal_name)s, NOW())
        ON CONFLICT (id) DO UPDATE SET
            internal_name = EXCLUDED.internal_name,
            synced_at     = NOW()
    """

    with conn.cursor() as cur:
        cur.executemany(query, groups)

    conn.commit()
    count = len(groups)
    logger.info("groups_dim_upserted", count=count)
    return count


def upsert_tags_dim(conn: psycopg.Connection, tags: list[dict]) -> int:
    """Upsert tag dimension records."""
    if not tags:
        return 0

    query = """
        INSERT INTO tags_dim (id, name, synced_at)
        VALUES (%(id)s, %(name)s, NOW())
        ON CONFLICT (id) DO UPDATE SET
            name      = EXCLUDED.name,
            synced_at = NOW()
    """

    with conn.cursor() as cur:
        cur.executemany(query, tags)

    conn.commit()
    count = len(tags)
    logger.info("tags_dim_upserted", count=count)
    return count


# ===========================================================================
# Phase 2: Fact / bridge table upserts
# ===========================================================================

def upsert_call_tags(conn: psycopg.Connection, call_tags: list[dict]) -> int:
    """
    Batch upsert call-tag bridge records.

    Uses DO NOTHING on conflict — the bridge row has no updateable fields.
    """
    if not call_tags:
        return 0

    query = """
        INSERT INTO call_tags (call_id, tag_id, tag_name)
        VALUES (%(call_id)s, %(tag_id)s, %(tag_name)s)
        ON CONFLICT (call_id, tag_id) DO NOTHING
    """

    with conn.cursor() as cur:
        cur.executemany(query, call_tags)

    conn.commit()
    count = len(call_tags)
    logger.info("call_tags_upserted", count=count)
    return count


def upsert_call_center_daily_stats(
    conn: psycopg.Connection, stats: list[dict], sync_date: date
) -> int:
    """Batch upsert call center daily aggregated statistics."""
    if not stats:
        return 0

    query = """
        INSERT INTO call_center_daily_stats (
            sync_date, group_id, group_name, country_code,
            total_calls, answered_calls, missed_calls,
            callback_calls, answer_rate_pct, synced_at
        ) VALUES (
            %(sync_date)s, %(group_id)s, %(group_name)s, %(country_code)s,
            %(total_calls)s, %(answered_calls)s, %(missed_calls)s,
            %(callback_calls)s, %(answer_rate_pct)s, NOW()
        )
        ON CONFLICT (sync_date, group_id) DO UPDATE SET
            group_name     = EXCLUDED.group_name,
            country_code   = EXCLUDED.country_code,
            total_calls    = EXCLUDED.total_calls,
            answered_calls = EXCLUDED.answered_calls,
            missed_calls   = EXCLUDED.missed_calls,
            callback_calls = EXCLUDED.callback_calls,
            answer_rate_pct = EXCLUDED.answer_rate_pct,
            synced_at      = NOW()
    """

    with conn.cursor() as cur:
        cur.executemany(query, stats)

    conn.commit()
    count = len(stats)
    logger.info("call_center_daily_stats_upserted", count=count, sync_date=str(sync_date))
    return count


def upsert_agent_daily_stats(
    conn: psycopg.Connection, stats: list[dict], sync_date: date
) -> int:
    """Batch upsert agent daily aggregated statistics."""
    if not stats:
        return 0

    query = """
        INSERT INTO agent_daily_stats (
            sync_date, agent_id, agent_name,
            presented_calls, answered_calls, total_talk_seconds, synced_at
        ) VALUES (
            %(sync_date)s, %(agent_id)s, %(agent_name)s,
            %(presented_calls)s, %(answered_calls)s, %(total_talk_seconds)s, NOW()
        )
        ON CONFLICT (sync_date, agent_id) DO UPDATE SET
            agent_name         = EXCLUDED.agent_name,
            presented_calls    = EXCLUDED.presented_calls,
            answered_calls     = EXCLUDED.answered_calls,
            total_talk_seconds = EXCLUDED.total_talk_seconds,
            synced_at          = NOW()
    """

    with conn.cursor() as cur:
        cur.executemany(query, stats)

    conn.commit()
    count = len(stats)
    logger.info("agent_daily_stats_upserted", count=count, sync_date=str(sync_date))
    return count


def upsert_call_reasons_daily(
    conn: psycopg.Connection, reasons: list[dict], sync_date: date
) -> int:
    """Batch upsert call reason tag counts per group per day."""
    if not reasons:
        return 0

    query = """
        INSERT INTO call_reasons_daily (
            sync_date, group_id, group_name, tag_id, tag_name, call_count, synced_at
        ) VALUES (
            %(sync_date)s, %(group_id)s, %(group_name)s,
            %(tag_id)s, %(tag_name)s, %(call_count)s, NOW()
        )
        ON CONFLICT (sync_date, group_id, tag_id) DO UPDATE SET
            group_name = EXCLUDED.group_name,
            tag_name   = EXCLUDED.tag_name,
            call_count = EXCLUDED.call_count,
            synced_at  = NOW()
    """

    with conn.cursor() as cur:
        cur.executemany(query, reasons)

    conn.commit()
    count = len(reasons)
    logger.info("call_reasons_daily_upserted", count=count, sync_date=str(sync_date))
    return count
