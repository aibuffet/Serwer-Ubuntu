from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import pandas as pd
import duckdb

# Argumenty DAG-a
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Tworzymy DAG
with DAG(
    dag_id="decision_maker_dag",
    description="Analiza sygnałów i wzbogacenie o TP/SL, dźwignię i czas trzymania",
    start_date=datetime(2025, 5, 28),
    schedule="0 10 * * *",  # codziennie o 10:00 UTC
    catchup=False,
    default_args=default_args,
    tags=["AI Buffett", "decision"]
) as dag:

    @task(task_id="enrich_signals")
    def enrich_signals():
        con = duckdb.connect("/home/ubuntu/data/market.duckdb")
        df = con.execute("SELECT * FROM ai_predictions").df()

        if df.empty:
            raise ValueError("Brak danych w tabeli ai_predictions")

        def enrich_trade_signals(df):
            df = df.copy()
            df["take_profit"] = None
            df["stop_loss"] = None
            df["leverage"] = None
            df["holding_time"] = None

            for idx, row in df.iterrows():
                close = row["close"]
                atr = row["atr"]
                signal = row["signal"]
                score = row.get("total_score", 0)
                rsi_score = row.get("rsi_score", 50)

                if signal == "LONG":
                    df.at[idx, "take_profit"] = round(close + atr * 2.5, 2)
                    df.at[idx, "stop_loss"] = round(close - atr * 1.5, 2)
                    df.at[idx, "leverage"] = 2 if rsi_score < 50 else 1
                    df.at[idx, "holding_time"] = 1 if score < 50 else 3

                elif signal == "SHORT":
                    df.at[idx, "take_profit"] = round(close - atr * 2.5, 2)
                    df.at[idx, "stop_loss"] = round(close + atr * 1.5, 2)
                    df.at[idx, "leverage"] = 2 if rsi_score > 50 else 1
                    df.at[idx, "holding_time"] = 1 if score < 50 else 3

                else:  # WAIT
                    df.at[idx, "take_profit"] = None
                    df.at[idx, "stop_loss"] = None
                    df.at[idx, "leverage"] = 1
                    df.at[idx, "holding_time"] = 0

            return df

        enriched_df = enrich_trade_signals(df)
        con.execute("CREATE OR REPLACE TABLE trade_enriched AS SELECT * FROM enriched_df")
        con.close()
        print("[OK] trade_enriched zapisane.")

    enrich_signals()
