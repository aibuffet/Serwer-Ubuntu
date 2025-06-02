from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import pandas as pd
import duckdb
import joblib
import boto3
import os
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

# AWS E2 konfiguracja
AWS_ACCESS_KEY = "N61heq4JcScOpIbzqrdW"
AWS_SECRET_KEY = "VKVaFGVDjQSIgkkjKYzmmdb5H7aLDsopAwQpmBJr"
E2_ENDPOINT = "g1k4.ie.idrivee2-22.com"
BUCKET_NAME = "ai-buffett-data"
PKL_FILENAME = "model.pkl"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="model_training_dag",
    description="Trening modelu AI na podstawie ai_signal_training_set i upload do E2",
    start_date=datetime(2025, 5, 28),
    schedule="0 2 * * *",  # codziennie o 2:00
    catchup=False,
    default_args=default_args,
    tags=["AI Buffett", "model"]
) as dag:

    @task(task_id="train_and_upload_model")
    def train_and_upload_model():
        con = duckdb.connect("/home/ubuntu/data/market.duckdb")
        df = con.execute("SELECT * FROM ai_signal_training_set").df()
        con.close()

        if df.empty:
            print("[WARN] Brak danych do treningu.")
            return

        features = [
            "open", "high", "low", "close", "volume", "rsi", "macd", "macd_signal", "macd_diff", "atr",
            "rsi_score", "macd_score", "volatility", "total_score", "finbert_score", "compound"
        ]

        if not set(features).issubset(set(df.columns)) or "signal" not in df.columns:
            raise ValueError("[ERROR] Brakuje wymaganych kolumn.")

        df = df.dropna(subset=features + ["signal"])
        X = df[features]
        y = df["signal"]

        if X.empty or len(X) < 10:
            print("[WARN] Za mało danych do treningu.")
            return

        X_train, _, y_train, _ = train_test_split(X, y, test_size=0.2, random_state=42)
        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)

        model = RandomForestClassifier(n_estimators=100, random_state=42)
        model.fit(X_train_scaled, y_train)

        # Zapis do pliku
        local_path = f"/tmp/{PKL_FILENAME}"
        joblib.dump((model, scaler), local_path)

        # Upload do E2 (IDrive)
        session = boto3.session.Session()
        s3 = session.client(
            service_name='s3',
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            endpoint_url=f"https://{E2_ENDPOINT}"
        )

        s3.upload_file(local_path, BUCKET_NAME, f"models/{PKL_FILENAME}")
        os.remove(local_path)

        print("[OK] Nowy model został wytrenowany i wysłany do E2")

    train_and_upload_model()
