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
            contact_id, contact_name, contact_company, synced_at
        ) VALUES (
            %(id)s, %(call_type)s, %(billsec)s, %(talking_time)s,
            %(waiting_time)s, %(wrapup_time)s, %(public_external)s,
            %(public_internal)s, %(country_code)s, %(recorded)s,
            %(is_voicemail)s, %(is_redirected)s, %(redirected_from)s,
            %(user_id)s, %(started_at)s, %(answered_at)s, %(ended_at)s,
            %(recording_link)s, %(call_status)s, %(call_date)s,
            %(contact_id)s, %(contact_name)s, %(contact_company)s, NOW()
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
