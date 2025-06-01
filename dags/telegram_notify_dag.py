from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import duckdb
import requests

# KONFIGURACJA
TELEGRAM_BOT_TOKEN = "7689792641:AAH87fcu7ATDAD0TVeU9DDPCUQb7cfb_wFE"
TELEGRAM_CHAT_ID = "965669451"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="telegram_notify_dag",
    description="Wysyła tylko ważne sygnały (LONG/SHORT) do Telegrama",
    start_date=datetime(2025, 5, 28),
    schedule="14,29,44,59 * * * *",  # możesz później zmienić na np. 0 9 * * * (raz dziennie)
    catchup=False,
    default_args=default_args,
    tags=["AI Buffett", "telegram"]
) as dag:

    @task(task_id="send_to_telegram")
    def send_to_telegram():
        con = duckdb.connect("/home/ubuntu/data/market.duckdb")
        df = con.execute("""
            SELECT * FROM trade_signals
            WHERE signal IN ('LONG', 'SHORT')
            ORDER BY total_score DESC
            LIMIT 5
        """).df()
        con.close()

        if df.empty:
            print("[INFO] Brak sygnałów LONG/SHORT do wysłania")
            return

        message = "\u2728 <b>AI Buffett - Sygnały Tradingowe</b> \u2728\n\n"
        for _, row in df.iterrows():
            message += f"<b>{row['ticker']}</b> | {row['signal']}\n"
            message += f"Score: {row['total_score']:.2f} | Sentiment: {row.get('finbert_score', 0):.2f}\n\n"

        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML"
        }
        response = requests.post(url, json=payload)

        if response.status_code == 200:
            print("[OK] Powiadomienie Telegram wysłane.")
        else:
            print(f"[ERROR] Telegram API: {response.text}")

    send_to_telegram()
