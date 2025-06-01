from airflow.decorators import dag, task
from datetime import datetime, timedelta
import requests
import duckdb
import os

# === KONFIGURACJA ===
EODHD_API_KEY = "682b8051412829.44572028"
TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "BTC-USD", "ETH-USD"]
DATABASE_PATH = "/home/ubuntu/data/market.duckdb"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

@dag(
    dag_id="intraday_ingest_eodhd",
    default_args=default_args,
    start_date=datetime(2025, 5, 1),
    schedule="*/15 * * * *",
    catchup=False,
    tags=["eodhd", "market", "duckdb"]
)
def ingest_intraday_data():

    @task()
    def fetch_and_store():
        os.makedirs(os.path.dirname(DATABASE_PATH), exist_ok=True)
        con = duckdb.connect(DATABASE_PATH)
        con.execute("""
            CREATE TABLE IF NOT EXISTS ohlc_intraday (
                ticker TEXT,
                datetime TIMESTAMP,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume DOUBLE
            )
        """)

        def safe(val):
            try:
                return float(val) if val not in ["NA", None] else None
            except:
                return None

        for ticker in TICKERS:
            try:
                url = f"https://eodhd.com/api/real-time/{ticker}?api_token={EODHD_API_KEY}&fmt=json"
                res = requests.get(url)
                data = res.json()

                o = safe(data.get("open"))
                h = safe(data.get("high"))
                l = safe(data.get("low"))
                c = safe(data.get("close"))
                v = safe(data.get("volume"))

                if None not in [o, h, l, c, v]:
                    con.execute("""
                        INSERT INTO ohlc_intraday VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (
                        ticker,
                        datetime.utcnow(),
                        o, h, l, c, v
                    ))
                    print(f"[OK] {ticker} zapisany.")
                else:
                    print(f"[SKIP] {ticker} – niepełne dane.")
            except Exception as e:
                print(f"[ERROR] {ticker}: {e}")

        con.close()

    fetch_and_store()

ingest_intraday_data()
