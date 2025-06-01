from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import pandas as pd
import duckdb
from ta.momentum import RSIIndicator
from ta.trend import MACD
from ta.volatility import AverageTrueRange

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="feature_engineering_technical",
    description="Oblicza wskaźniki RSI, MACD, ATR na danych z DuckDB",
    start_date=datetime(2025, 5, 28),
    catchup=False,
    schedule="*/15 * * * *",  # Wykonuj co 15 minut
    default_args=default_args,
)

@task(task_id="calculate_features", dag=dag)
def calculate_features():
    con = duckdb.connect("/home/ubuntu/data/market.duckdb")
    df = con.execute("SELECT * FROM ohlc_intraday").df()

    def compute_indicators(group):
        if len(group) < 5:
            return pd.DataFrame()
        group = group.sort_values("datetime").copy()

        group["rsi"] = RSIIndicator(close=group["close"], window=5).rsi()
        macd = MACD(close=group["close"], window_slow=26, window_fast=12, window_sign=9)
        group["macd"] = macd.macd()
        group["macd_signal"] = macd.macd_signal()
        group["macd_diff"] = macd.macd_diff()
        group["atr"] = AverageTrueRange(high=group["high"], low=group["low"], close=group["close"], window=5).average_true_range()

        return group

    result = df.groupby("ticker").apply(compute_indicators).reset_index(drop=True)
    con.execute("CREATE OR REPLACE TABLE feature_data AS SELECT * FROM result")
    con.close()
    print("[OK] feature_data zapisane.")

calculate_features()
