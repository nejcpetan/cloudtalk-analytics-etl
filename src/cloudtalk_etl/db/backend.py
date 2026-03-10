"""
Database backend selector.

Reads DB_BACKEND from the environment (default: "postgresql") and re-exports
get_connection, ensure_schema, and the three upsert functions from the matching
backend module. All other code imports from here — never from the backend
modules directly — so switching databases only requires changing one env var.

Supported values:
    postgresql  (default) — psycopg3, Neon / any PostgreSQL
    mysql                 — mysql-connector-python, MySQL 8.0+
"""
import os

_backend = os.environ.get("DB_BACKEND", "postgresql").lower()

if _backend == "mysql":
    from cloudtalk_etl.db.connection_mysql import get_connection
    from cloudtalk_etl.db.schema_mysql import ensure_schema
    from cloudtalk_etl.db.repositories_mysql import (
        upsert_call_center_groups,
        upsert_agent_stats,
        upsert_call_reasons,
    )
else:
    from cloudtalk_etl.db.connection import get_connection          # type: ignore[assignment]
    from cloudtalk_etl.db.schema import ensure_schema               # type: ignore[assignment]
    from cloudtalk_etl.db.repositories import (                     # type: ignore[assignment]
        upsert_call_center_groups,
        upsert_agent_stats,
        upsert_call_reasons,
    )

__all__ = [
    "get_connection",
    "ensure_schema",
    "upsert_call_center_groups",
    "upsert_agent_stats",
    "upsert_call_reasons",
]
