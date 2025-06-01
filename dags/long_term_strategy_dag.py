from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import duckdb
import pandas as pd
import os
import boto3
from botocore.exceptions import NoCredentialsError

# Ścieżka pliku lokalnego i klucz S3 (ścieżka w bucket)
LOCAL_PATH = "/home/ubuntu/data/longterm_signal_history.parquet"
S3_KEY = "longterm/longterm_signal_history.parquet"

# Domyślne argumenty dla DAGa
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="long_term_strategy_dag",
    description="Strategia długoterminowa + backup do IDrive E2",
    start_date=datetime(2025, 5, 28),
    schedule="0 3 * * *",
    catchup=False,
    default_args=default_args,
    tags=["AI Buffett", "longterm"]
) as dag:

    @task(task_id="generate_longterm_signals")
    def generate_signals():
        con = duckdb.connect("/home/ubuntu/data/market.duckdb")
        df = con.execute("SELECT AVG(finbert_score) as avg_score FROM sentiment_scores").fetchdf()

        avg_score = df.iloc[0]['avg_score']
        signal = "BULLISH" if avg_score > 0.6 else "BEARISH" if avg_score < 0.4 else "NEUTRAL"
        ts = datetime.utcnow()

        con.execute("""
            CREATE TABLE IF NOT EXISTS longterm_signal_history (
                datetime TIMESTAMP,
                signal VARCHAR,
                finbert_score DOUBLE
            )
        """)
        con.execute(f"""
            INSERT INTO longterm_signal_history
            VALUES ('{ts}', '{signal}', {avg_score})
        """)
        con.execute(f"COPY (SELECT * FROM longterm_signal_history) TO '{LOCAL_PATH}' (FORMAT 'parquet')")
        con.close()

        print(f"[INFO] Średni sentyment: {avg_score:.2f}")
        print(f"[OK] Zapisano sygnał: {signal}")

    @task(task_id="backup_to_idrive_e2")
    def backup_to_e2():
        access_key = os.getenv("IDRIVE_E2_ACCESS_KEY_ID")
        secret_key = os.getenv("IDRIVE_E2_SECRET_ACCESS_KEY")
        endpoint = os.getenv("IDRIVE_E2_ENDPOINT")
        bucket = os.getenv("IDRIVE_E2_BUCKET")

        print("DEBUG: ACCESS_KEY=", access_key)
        print("DEBUG: SECRET_KEY=", "SET" if secret_key else "MISSING")
        print("DEBUG: ENDPOINT=", endpoint)
        print("DEBUG: BUCKET=", bucket)

        if not access_key or not secret_key or not endpoint or not bucket:
            print("[ERROR] Brak poświadczeń IDrive E2")
            return

        s3 = boto3.client(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            endpoint_url=f"https://{endpoint}"
        )
        try:
            s3.upload_file(LOCAL_PATH, bucket, S3_KEY)
            print(f"[OK] Wysłano do E2: {bucket}/{S3_KEY}")
        except FileNotFoundError:
            print("[ERROR] Nie znaleziono pliku lokalnego")
        except NoCredentialsError:
            print("[ERROR] Brak poprawnych danych logowania")

    generate_signals() >> backup_to_e2()
