import structlog
from urllib.parse import urlparse, parse_qs

logger = structlog.get_logger()


def get_connection(database_url: str):
    """
    Create a connection to a MySQL database.

    Expects a standard mysql:// URL:
        mysql://user:password@host:3306/dbname
        mysql://user:password@host:3306/dbname?ssl_disabled=true

    SSL is enabled by default. Pass ?ssl_disabled=true to disable it
    (e.g. for local development without certificates).
    """
    try:
        import mysql.connector
    except ImportError:
        raise ImportError(
            "mysql-connector-python is required for the MySQL backend. "
            "Rebuild the Docker image with: --build-arg INSTALL_TARGET='.[mysql]'"
        )

    parsed = urlparse(database_url)
    query_params = parse_qs(parsed.query)
    ssl_disabled = query_params.get("ssl_disabled", ["false"])[0].lower() == "true"

    conn = mysql.connector.connect(
        host=parsed.hostname,
        port=parsed.port or 3306,
        user=parsed.username,
        password=parsed.password,
        database=parsed.path.lstrip("/"),
        autocommit=False,
        ssl_disabled=ssl_disabled,
    )
    logger.info("database_connected", server=parsed.hostname, backend="mysql")
    return conn
