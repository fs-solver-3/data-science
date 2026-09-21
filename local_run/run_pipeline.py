"""Run the whole pipeline locally: producer -> bus -> consumer -> ETL.

Producer and consumer run concurrently in threads, so data really does stream
through the bus rather than being staged. The ETL runs afterwards, once
ingestion is complete -- unlike docker-compose, which fires it on a 300s timer
regardless of whether ingestion has finished.

Usage:
    python run_pipeline.py              # full dataset (527,040 rows)
    python run_pipeline.py --limit 5000 # quick smoke test
"""
import argparse
import sqlite3
import threading
import time

import database_local as database
from message_bus import MessageBus
from producer_local import run_producer
from consumer_local import run_consumer
from etl_local import run_etl_process


def verify(db_path):
    """Read back what actually landed in the database."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    print("\n" + "=" * 62)
    print(" VERIFICATION - querying the database directly")
    print("=" * 62)

    for table in ("synthetic_data", "stock_price_summary", "sales_trend_summary"):
        try:
            count = cur.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"\n{table}: {count:,} rows")
            cols = [d[1] for d in cur.execute(f"PRAGMA table_info({table})").fetchall()]
            print(f"  columns: {', '.join(cols)}")
            for row in cur.execute(f"SELECT * FROM {table} LIMIT 3").fetchall():
                print(f"  {row}")
        except Exception as e:
            print(f"\n{table}: ERROR {e}")

    conn.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None,
                    help="only stream the first N rows")
    args = ap.parse_args()

    started = time.time()
    database.reset_database()

    bus = MessageBus(maxsize=10_000)
    results = {}

    producer_thread = threading.Thread(
        target=lambda: results.update(produced=run_producer(bus, limit=args.limit)),
        name="producer",
    )
    consumer_thread = threading.Thread(
        target=lambda: results.update(consumed=run_consumer(bus)),
        name="consumer",
    )

    print("=" * 62)
    print(" STREAMING PHASE (producer + consumer running concurrently)")
    print("=" * 62)
    consumer_thread.start()
    producer_thread.start()
    producer_thread.join()
    consumer_thread.join()
    stream_secs = time.time() - started

    print("\n" + "=" * 62)
    print(" BATCH PHASE (ETL)")
    print("=" * 62)
    etl_started = time.time()
    run_etl_process()
    etl_secs = time.time() - etl_started

    verify(database.DB_PATH)

    produced = results.get("produced", 0)
    consumed = results.get("consumed", 0)
    print("\n" + "=" * 62)
    print(" SUMMARY")
    print("=" * 62)
    print(f"  produced : {produced:,}")
    print(f"  inserted : {consumed:,}")
    print(f"  match    : {'YES' if produced == consumed else 'NO - MESSAGES LOST'}")
    print(f"  streaming: {stream_secs:.1f}s")
    print(f"  etl      : {etl_secs:.1f}s")
    print(f"  total    : {time.time() - started:.1f}s")
    print(f"  database : {database.DB_PATH}")


if __name__ == "__main__":
    main()
