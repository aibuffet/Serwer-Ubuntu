# /home/ubuntu/airflow/dags/ai_predict_dag.py

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
    description="Ładowanie modelu AI i generowanie predykcji z confidence, TP/SL, leverage",
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
        y_proba = model.predict_proba(X)

        latest_df["signal"] = y_pred
        latest_df["confidence_score"] = y_proba.max(axis=1)

        # TP = ATR * 3, SL = ATR * 2
        latest_df["tp"] = latest_df["atr"] * 3
        latest_df["sl"] = latest_df["atr"] * 2

        # Dźwignia: 0.5 => 1x, 1.0 => 50x
        latest_df["leverage"] = latest_df["confidence_score"].apply(lambda x: min(1 + (x - 0.5) * 98, 50))

        con.execute("CREATE OR REPLACE TABLE ai_predictions AS SELECT * FROM latest_df")
        con.close()

        print("[OK] Zapisano predykcje z scoringiem do DuckDB")
        print(latest_df[["ticker", "datetime", "signal", "confidence_score", "tp", "sl", "leverage"]])

    run_predictions()
