from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import pandas as pd
import duckdb
import joblib
import os

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ai_predict_dag",
    description="Ładowanie modelu AI i generowanie predykcji",
    start_date=datetime(2025, 5, 28),
    schedule="15 * * * *",
    catchup=False,
    default_args=default_args,
    tags=["AI Buffett", "predict"]
) as dag:

    @task(task_id="run_predictions")
    def run_predictions():
        model_path = "/home/ubuntu/models/final_model.pkl"
        if not os.path.exists(model_path):
            raise FileNotFoundError(f"Nie znaleziono modelu: {model_path}")

        model = joblib.load(model_path)
        con = duckdb.connect("/home/ubuntu/data/market.duckdb")

        df = con.execute("SELECT * FROM trade_signals").df()

        if df.empty:
            raise ValueError("Brak danych w tabeli trade_signals")

        latest_df = df.sort_values("datetime").groupby("ticker").tail(1).copy()

        features = [
            "open", "high", "low", "close", "volume", "rsi", "macd", "macd_signal", "macd_diff", "atr",
            "rsi_score", "macd_score", "volatility", "total_score",
            "finbert_score", "compound"
        ]

        missing = [col for col in features if col not in latest_df.columns]
        if missing:
            raise ValueError(f"Brakuje kolumn w danych: {missing}")

        X = latest_df[features]
        y_pred = model.predict(X)
        latest_df["signal"] = y_pred

        con.execute("CREATE OR REPLACE TABLE ai_predictions AS SELECT * FROM latest_df")
        con.close()

        print("[OK] Zapisano predykcje do DuckDB")
        print(latest_df[["ticker", "datetime", "signal"]])

    run_predictions()
