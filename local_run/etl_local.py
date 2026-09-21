"""Local stand-in for etl/etl.py.

Same Extract -> Transform -> Load, same 10-minute stock_price and 20-minute
sales_trend summaries, against SQLite instead of PostgreSQL.

Kept faithful to the original on purpose, including the column naming: the
original renames to interval_start / avg_stock_price and writes with
if_exists='replace', which drops the tables init.sql created and rebuilds
them with these names. That inconsistency is reproduced here rather than
quietly fixed, so this run reflects what the real pipeline does.
"""
import warnings

import pandas as pd
from sqlalchemy import create_engine

import database_local as database

warnings.filterwarnings("ignore")


def run_etl_process(db_path=database.DB_PATH):
    print("[etl] starting ETL process...")

    conn, cursor = database.get_db_connection(db_path)
    if not conn:
        print("[etl] failed to connect to the database. exiting.")
        return None

    try:
        print("[etl] extracting data from 'synthetic_data'...")
        df = pd.read_sql("SELECT * FROM synthetic_data", conn)
        print(f"[etl] extracted {len(df):,} rows.")

        if df.empty:
            print("[etl] no data found in 'synthetic_data'. exiting.")
            return None

        print("[etl] transforming...")
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        print(f"[etl] total missing values: {df.isnull().sum().sum()}")

        df.drop(columns=["id"], inplace=True)

        dupes = int(df.duplicated().sum())
        print(f"[etl] total duplicated: {dupes}")
        if dupes > 0:
            df.drop_duplicates(inplace=True)
            print(f"[etl] {dupes} duplicated rows dropped.")

        print("[etl] calculating 10-minute summary for stock_price...")
        stock_price_summary = (
            df.assign(interval_start=df["timestamp"].dt.floor("10min"))
            .groupby("interval_start")["stock_price"]
            .mean()
            .reset_index()
            .rename(columns={"stock_price": "avg_stock_price"})
        )

        print("[etl] calculating 20-minute summary for sales_trend...")
        sales_trend_summary = (
            df.assign(interval_start=df["timestamp"].dt.floor("20min"))
            .groupby("interval_start")["sales_trend"]
            .mean()
            .reset_index()
            .rename(columns={"sales_trend": "avg_sales_trend"})
        )

        print("[etl] loading summaries back into the database...")
        engine = create_engine(f"sqlite:///{db_path}")

        stock_price_summary.to_sql(
            "stock_price_summary", engine, if_exists="replace", index=False
        )
        print(f"[etl] inserted {len(stock_price_summary):,} rows into 'stock_price_summary'.")

        sales_trend_summary.to_sql(
            "sales_trend_summary", engine, if_exists="replace", index=False
        )
        print(f"[etl] inserted {len(sales_trend_summary):,} rows into 'sales_trend_summary'.")

        print("[etl] ETL process completed successfully.")
        return stock_price_summary, sales_trend_summary

    except Exception as e:
        print(f"[etl] error during ETL process: {e}")
        return None

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    run_etl_process()
