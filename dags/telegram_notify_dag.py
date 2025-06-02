from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import duckdb
import requests

TELEGRAM_BOT_TOKEN = "7689792641:AAH87fcu7ATDAD0TVeU9DDPCUQb7cfb_wFE"
TELEGRAM_CHAT_ID = "965669451"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="telegram_notify_dag",
    description="Wysyła unikalne sygnały z ai_signals_final do Telegrama (zabezpieczenie sent=true)",
    start_date=datetime(2025, 5, 28),
    schedule="*/15 * * * *",
    catchup=False,
    default_args=default_args,
    tags=["AI Buffett", "telegram"]
) as dag:

    @task(task_id="send_unique_signals")
    def send_unique_signals():
        con = duckdb.connect("/home/ubuntu/data/market.duckdb")

        # Upewnij się, że kolumna sent istnieje (jeśli nie, dodaj)
        con.execute("""
            ALTER TABLE ai_signals_final ADD COLUMN IF NOT EXISTS sent BOOLEAN;
        """)

        # Pobierz tylko nie-wysłane
        df = con.execute("""
            SELECT * FROM ai_signals_final
            WHERE sent IS NULL OR sent = FALSE
        """).df()

        if df.empty:
            print("[INFO] Brak nowych sygnałów do wysłania.")
            return

        for _, row in df.iterrows():
            ticker = row["ticker"]
            signal = row["signal"]
            conf = round(row.get("confidence_score", 0), 2)
            tp = round(row.get("take_profit", 0), 2)
            sl = round(row.get("stop_loss", 0), 2)
            lev = int(row.get("leverage", 1))
            hold = int(row.get("holding_time", 0))
            date = row["datetime"]

            message = f"""📈 <b>AI Buffett: {signal} na {ticker}</b>\n
Confidence: <b>{conf}</b>\n🎯 TP: {tp} | 🛡️ SL: {sl}
⚡ Leverage: {lev}x | ⏱️ Holding: {hold}h"""

            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            payload = {
                "chat_id": TELEGRAM_CHAT_ID,
                "text": message,
                "parse_mode": "HTML"
            }
            r = requests.post(url, json=payload)

            if r.status_code == 200:
                print(f"[OK] Telegram wysłany: {ticker}")
                # oznacz jako wysłane
                con.execute(f"""
                    UPDATE ai_signals_final
                    SET sent = true
                    WHERE ticker = '{ticker}' AND datetime = '{date}'
                """)
            else:
                print(f"[ERROR] Telegram API: {r.text}")

        con.close()

    send_unique_signals()
