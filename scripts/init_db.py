"""
One-time database initialisation script.

Usage:
    python scripts/init_db.py

Reads DATABASE_URL from the environment (or .env file) and creates all
tables defined in the technical specification. Safe to re-run — uses
CREATE TABLE IF NOT EXISTS throughout.
"""
import sys
import os

# Ensure the src package is importable when run directly from the project root.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from cloudtalk_etl.config import Settings  # noqa: E402
from cloudtalk_etl.db.connection import get_connection  # noqa: E402
from cloudtalk_etl.db.schema import ensure_schema  # noqa: E402

if __name__ == "__main__":
    settings = Settings()
    print(f"Connecting to database...")
    conn = get_connection(settings.database_url)
    try:
        ensure_schema(conn)
        print("Schema initialised successfully.")
    finally:
        conn.close()
