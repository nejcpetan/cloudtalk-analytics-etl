import psycopg
import structlog

logger = structlog.get_logger()


def get_connection(database_url: str) -> psycopg.Connection:
    """
    Create a connection to Neon PostgreSQL.

    Neon requires SSL. The connection string should include sslmode=require.
    Uses autocommit=False for transactional writes.
    """
    conn = psycopg.connect(
        database_url,
        autocommit=False,
    )
    logger.info("database_connected", server=conn.info.host)
    return conn
