from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import duckdb
import pandas as pd

# Argumenty domyślne
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Definicja DAG-a
with DAG(
    dag_id="ai_signal_learning_dag",
    description="Uczenie AI na podstawie sygnałów LONG/SHORT z trade_signals",
    start_date=datetime(2025, 5, 28),
    catchup=False,
    schedule=None,
    default_args=default_args,
    tags=["ai", "learning", "buffett"]
) as dag:

    @task(task_id="analyze_signals")
    def analyze_signals():
        con = duckdb.connect("/home/ubuntu/data/market.duckdb")
        df = con.execute("SELECT * FROM trade_signals").df()

        if df.empty:
            raise ValueError("Brak danych w trade_signals")

        df["entry_price"] = df.apply(
            lambda row: (row["open"] + row["close"]) / 2 if row["signal"] in ["LONG", "SHORT"] else None,
            axis=1
        )

        df["exit_price"] = df.apply(
            lambda row: row["entry_price"] + row["atr"] if row["signal"] == "LONG"
            else row["entry_price"] - row["atr"] if row["signal"] == "SHORT" else None,
            axis=1
        )

        df["holding_time"] = 60 * 60 * 4  # 4h - domyślna wartość (może być dynamiczna w przyszłości)

        con.execute("CREATE OR REPLACE TABLE ai_signal_training_set AS SELECT * FROM df")
        con.close()
        print("[OK] Zapisano ai_signal_training_set do DuckDB")

    analyze_signals()
