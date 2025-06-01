from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import pandas as pd
import duckdb

# Domyślne argumenty DAG
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Tworzenie DAG-a
with DAG(
    dag_id="ranking_filter_dag",
    description="Analizuje dane techniczne i wybiera najlepsze tickery",
    start_date=datetime(2025, 5, 28),
    schedule="12,27,42,57 * * * *",
    catchup=False,
    default_args=default_args,
    tags=["AI Buffett", "scoring", "ranking"]
) as dag:

    @task(task_id="rank_and_filter")
    def rank_and_filter():
        con = duckdb.connect("/home/ubuntu/data/market.duckdb")
        df = con.execute("SELECT * FROM feature_data").df()

        if df.empty:
            raise ValueError("Brak danych w tabeli feature_data")

        # Przekształcenie: wybieramy ostatni wiersz dla każdego tickera
        latest_df = df.sort_values("datetime").groupby("ticker").tail(1).copy()

        # Normalizacja RSI do scoringu (wyższy RSI = wyższy potencjał short)
        latest_df["rsi_score"] = 100 - latest_df["rsi"]

        # Scoring MACD (gdy diff > 0 i MACD > sygnał = potencjalny long)
        latest_df["macd_score"] = latest_df["macd_diff"].apply(lambda x: 1 if x > 0 else 0)

        # ATR jako zmienność – im wyższy ATR, tym wyższy potencjał do TP/SL
        latest_df["volatility"] = latest_df["atr"]

        # Finalny score (można dostroić wagami)
        latest_df["total_score"] = (
            latest_df["rsi_score"] * 0.4 +
            latest_df["macd_score"] * 0.4 +
            latest_df["volatility"] * 0.2
        )

        # Filtrowanie top wyników
        top_df = latest_df.sort_values("total_score", ascending=False).head(10)

        con.execute("CREATE OR REPLACE TABLE ranking_filtered AS SELECT * FROM top_df")
        con.close()
        print("[OK] Zapisano ranking_filtered w DuckDB")

    rank_and_filter()
