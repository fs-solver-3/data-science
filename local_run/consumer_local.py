"""Local stand-in for consumer/consumer.py.

Polls the bus, decodes each JSON message and inserts it into SQLite, exactly
as the original does against Kafka + PostgreSQL. Commits are batched -- see
the note in database_local.insert_into_db.
"""
import json

from message_bus import EOS
import database_local as database

COMMIT_EVERY = 5_000


def run_consumer(bus, progress_every=50_000):
    conn, cursor = database.get_db_connection()
    if conn is None:
        print("[consumer] no database connection; aborting.")
        return 0

    print("[consumer] started. waiting for messages...")
    inserted = 0
    errors = 0

    try:
        while True:
            msg = bus.poll(timeout=1.0)

            if msg is None:
                # Same as the original: nothing this poll window, keep going.
                continue

            if msg is EOS:
                print("[consumer] end of stream reached.")
                break

            try:
                data = json.loads(msg.decode("utf-8"))
                database.insert_into_db(
                    cursor,
                    data["timestamp"],
                    data["stock_price"],
                    data["sales_trend"],
                )
                inserted += 1

                if inserted % COMMIT_EVERY == 0:
                    conn.commit()
                if inserted % progress_every == 0:
                    print(f"[consumer] {inserted:,} rows inserted")

            except (ValueError, KeyError) as e:
                errors += 1
                print(f"[consumer] error parsing message: {e}")

        conn.commit()

    finally:
        conn.commit()
        cursor.close()
        conn.close()
        print(f"[consumer] finished. {inserted:,} rows inserted, {errors} errors.")

    return inserted
