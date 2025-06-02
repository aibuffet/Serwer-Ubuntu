from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import duckdb
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
import joblib
import os

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ai_signal_learning_dag",
    description="Uczenie modelu AI na podstawie ai_signals_final",
    start_date=datetime(2025, 5, 28),
    schedule="0 2 * * *",  # codziennie o 02:00
    catchup=False,
    default_args=default_args,
    tags=["ai", "learning", "buffett"]
) as dag:

    @task(task_id="train_ai_model")
    def train_ai_model():
        con = duckdb.connect("/home/ubuntu/data/market.duckdb")
        df = con.execute("SELECT * FROM ai_signals_final WHERE decision = 'YES'").df()
        con.close()

        if df.empty:
            print("[INFO] Brak danych do trenowania.")
            return

        # Cechy do uczenia
        features = [
            "open", "high", "low", "close", "volume", "rsi", "macd", "macd_signal", "macd_diff", "atr",
            "rsi_score", "macd_score", "volatility", "total_score",
            "finbert_score", "compound"
        ]

        missing = [f for f in features if f not in df.columns]
        if missing:
            raise ValueError(f"Brakuje kolumn: {missing}")

        df = df.dropna(subset=features + ["signal"])
        X = df[features]
        y = df["signal"]

        # Trening modelu
        model = RandomForestClassifier(n_estimators=100, random_state=42)
        model.fit(X, y)

        # Zapisz model
        os.makedirs("/home/ubuntu/models", exist_ok=True)
        joblib.dump(model, "/home/ubuntu/models/final_model.pkl")

        print("[OK] Model AI został wytrenowany i zapisany jako final_model.pkl")

    train_ai_model()
