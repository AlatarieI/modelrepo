# database_manager.py
"""
Database manager using psycopg2.
Provides safe connection handling and atomic insert for Model.
Configure DB connection via environment variables or edit DB_CONFIG below.
"""

import os
import psycopg2
from contextlib import contextmanager

DB_CONFIG = {
    "host": os.getenv("MODELDB_HOST", "localhost"),
    "dbname": os.getenv("MODELDB_NAME", "pgr_webapp"),
    "user": os.getenv("MODELDB_USER", "myuser"),
    "password": os.getenv("MODELDB_PASSWORD", "o4Wg4Kuh"),
    "port": int(os.getenv("MODELDB_PORT", 5432)),
}


@contextmanager
def get_connection():
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        yield conn
    finally:
        try:
            conn.close()
        except Exception:
            pass


def init_database():
    """Create tables if they don't exist (safe)."""
    schema_sql = """
    CREATE TABLE IF NOT EXISTS Model (
        model_id SERIAL PRIMARY KEY,
        model_name VARCHAR(255) NOT NULL UNIQUE,
        format VARCHAR(50),
        source_url VARCHAR(255),
        download_date DATE,
        created_by VARCHAR(255),
        created_in VARCHAR(100),
        uploaded_by VARCHAR(255),
        model_description TEXT,
        polygon_count INTEGER,
        preview_file VARCHAR(255) NOT NULL,
        average_rating FLOAT DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS Tag (
        tag_id SERIAL PRIMARY KEY,
        tag_name VARCHAR(100) UNIQUE NOT NULL
    );

    CREATE TABLE IF NOT EXISTS Rating (
        rating_id SERIAL PRIMARY KEY,
        model_id INTEGER NOT NULL REFERENCES Model(model_id) ON DELETE CASCADE,
        rating_value INTEGER CHECK (rating_value BETWEEN 1 AND 5),
        cookie_id VARCHAR(128)
    );

    CREATE TABLE IF NOT EXISTS ModelTag (
        model_id INTEGER NOT NULL REFERENCES Model(model_id) ON DELETE CASCADE,
        tag_id INTEGER NOT NULL REFERENCES Tag(tag_id) ON DELETE CASCADE,
        PRIMARY KEY (model_id, tag_id)
    );

    CREATE INDEX IF NOT EXISTS idx_model_download_date ON Model(download_date);
    CREATE INDEX IF NOT EXISTS idx_model_polygon_count ON Model(polygon_count);
    CREATE INDEX IF NOT EXISTS idx_model_format ON Model(format);
    CREATE INDEX IF NOT EXISTS idx_rating_model_id ON Rating(model_id);
    """
    with get_connection() as conn:
        with conn:
            with conn.cursor() as cur:
                cur.execute(schema_sql)
    print("✅ Database initialized / schema ensured.")


def insert_model(model_data: dict) -> int:
    """
    Insert a model into Model table. Uses a transaction; commit only if entire operation succeeds.
    model_data keys:
      - model_name (str)
      - format (str) optional
      - source_url (str) optional
      - download_date (datetime.date) optional
      - created_by (str) optional
      - created_in (str) optional
      - uploaded_by (str) optional
      - model_description (str) optional
      - polygon_count (int) optional
      - preview_file (str) REQUIRED (filename stored relative to model folder)
    Returns new model_id.
    Raises exception on failure; caller should handle rollback/cleanup.
    """
    required = ["model_name", "preview_file"]
    for r in required:
        if r not in model_data or not model_data[r]:
            raise ValueError(f"Missing required field: {r}")

    sql_insert = """
        INSERT INTO Model (
            model_name, format, source_url, download_date, created_by, created_in,
            uploaded_by, model_description, polygon_count, preview_file
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING model_id;
    """

    with get_connection() as conn:
        with conn:  
            with conn.cursor() as cur:
                cur.execute(sql_insert, (
                    model_data.get("model_name"),
                    model_data.get("format"),
                    model_data.get("source_url"),
                    model_data.get("download_date"),
                    model_data.get("created_by"),
                    model_data.get("created_in"),
                    model_data.get("uploaded_by"),
                    model_data.get("model_description"),
                    model_data.get("polygon_count"),
                    model_data.get("preview_file")
                ))
                model_id = cur.fetchone()[0]
                return model_id
