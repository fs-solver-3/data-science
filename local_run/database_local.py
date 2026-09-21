"""SQLite stand-in for database/database.py.

Mirrors the same two functions (get_db_connection / insert_into_db) so the
consumer code reads the same way it does against PostgreSQL. The schema is a
direct translation of init.sql.
"""
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent / "kafka_data.db"

# Direct translation of init.sql. SERIAL -> INTEGER AUTOINCREMENT,
# TIMESTAMP -> TEXT (SQLite has no native timestamp type).
SCHEMA = """
CREATE TABLE IF NOT EXISTS synthetic_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT,
    stock_price REAL,
    sales_trend REAL
);

CREATE TABLE IF NOT EXISTS stock_price_summary (
    timestamp TEXT PRIMARY KEY,
    stock_price_avg REAL
);

CREATE TABLE IF NOT EXISTS sales_trend_summary (
    timestamp TEXT PRIMARY KEY,
    sales_trend_avg REAL
);
"""


def get_db_connection(db_path=DB_PATH):
    """Open the database and ensure the schema exists."""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.executescript(SCHEMA)
        conn.commit()
        print(f"Database connection established: {db_path}")
        return conn, cursor
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None, None


def insert_into_db(cursor, timestamp, stock_price, sales_trend):
    """Insert one row. Commit is handled by the caller in batches.

    NOTE: the original database.py commits on every single row. That is left
    to the consumer here so a 527k-row run does not do 527k commits.
    """
    cursor.execute(
        "INSERT INTO synthetic_data (timestamp, stock_price, sales_trend) VALUES (?, ?, ?)",
        (timestamp, stock_price, sales_trend),
    )


def reset_database(db_path=DB_PATH):
    """Drop the file so each pipeline run starts clean."""
    if Path(db_path).exists():
        Path(db_path).unlink()
        print(f"Removed existing database: {db_path}")
