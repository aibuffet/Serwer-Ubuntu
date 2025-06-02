# /home/ubuntu/airflow/dags/decision_maker_dag.py

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
    description="Analiza sygnałów, wzbogacenie o TP/SL, dźwignię, czas trzymania i decyzję YES/NO",
    start_date=datetime(2025, 5, 28),
    schedule="10 * * * *",  # co godzinę o minutę 10
    catchup=False,
    default_args=default_args,
    tags=["AI Buffett", "decision"]
) as dag:

    @task(task_id="enrich_and_decide")
    def enrich_and_decide():
        con = duckdb.connect("/home/ubuntu/data/market.duckdb")
        df = con.execute("SELECT * FROM ai_predictions").df()

        if df.empty:
            raise ValueError("Brak danych w tabeli ai_predictions")

        def enrich_and_filter(df):
            df = df.copy()
            df["take_profit"] = None
            df["stop_loss"] = None
            df["leverage"] = None
            df["holding_time"] = None
            df["decision"] = "NO"

            for idx, row in df.iterrows():
                close = row["close"]
                atr = row["atr"]
                signal = row["signal"]
                score = row.get("total_score", 0)
                rsi_score = row.get("rsi_score", 50)
                confidence = row.get("confidence_score", 0)

                decision = "NO"

                if signal in ["LONG", "SHORT"] and confidence >= 0.7:
                    if signal == "LONG":
                        tp = close + atr * 2.5
                        sl = close - atr * 1.5
                        leverage = 2 if rsi_score < 50 else 1
                    elif signal == "SHORT":
                        tp = close - atr * 2.5
                        sl = close + atr * 1.5
                        leverage = 2 if rsi_score > 50 else 1

                    holding_time = 1 if score < 50 else 3

                    if leverage >= 1:
                        decision = "YES"

                    df.at[idx, "take_profit"] = round(tp, 2)
                    df.at[idx, "stop_loss"] = round(sl, 2)
                    df.at[idx, "leverage"] = leverage
                    df.at[idx, "holding_time"] = holding_time
                    df.at[idx, "decision"] = decision

            return df[df["decision"] == "YES"]

        result = enrich_and_filter(df)

        con.execute("CREATE OR REPLACE TABLE ai_signals_final AS SELECT * FROM result")
        con.close()

        print("[OK] Zapisano decyzje do ai_signals_final:")
        print(result[["ticker", "signal", "confidence_score", "leverage", "decision"]])

    enrich_and_decide()
