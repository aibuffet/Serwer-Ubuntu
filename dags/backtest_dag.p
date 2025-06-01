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
    dag_id="backtest_dag",
    description="Backtest strategii AI Buffetta na danych historycznych",
    start_date=datetime(2025, 5, 28),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["AI Buffett", "backtest"]
) as dag:

    @task(task_id="run_backtest")
    def run_backtest():
        con = duckdb.connect("/home/ubuntu/data/market.duckdb")

        # Pobranie danych: sygnały handlowe + ceny
        signals_df = con.execute("SELECT * FROM trade_signals").df()

        if signals_df.empty:
            raise ValueError("Brak danych w tabeli trade_signals")

        results = []
        for _, row in signals_df.iterrows():
            ticker = row["ticker"]
            entry_price = row["close"]
            signal = row["signal"]
            score = row["total_score"]
            sentiment = row["finbert_score"]
            datetime_open = pd.to_datetime(row["datetime"])

            # Szacowany TP/SL oparty na ATR
            atr = row["atr"]
            tp = entry_price + atr * 2 if signal == "LONG" else entry_price - atr * 2
            sl = entry_price - atr * 1.5 if signal == "LONG" else entry_price + atr * 1.5

            # Szukanie momentu wyjścia z pozycji w danych
            future_df = signals_df[
                (signals_df["ticker"] == ticker) &
                (pd.to_datetime(signals_df["datetime"]) > datetime_open)
            ].sort_values("datetime")

            exit_price = None
            exit_time = None
            for _, frow in future_df.iterrows():
                price = frow["close"]
                time = frow["datetime"]
                if signal == "LONG" and (price >= tp or price <= sl):
                    exit_price = price
                    exit_time = time
                    break
                if signal == "SHORT" and (price <= tp or price >= sl):
                    exit_price = price
                    exit_time = time
                    break

            result = {
                "ticker": ticker,
                "signal": signal,
                "entry_price": entry_price,
                "exit_price": exit_price,
                "score": score,
                "sentiment": sentiment,
                "datetime_open": datetime_open,
                "datetime_close": exit_time,
                "tp": tp,
                "sl": sl
            }
            results.append(result)

        results_df = pd.DataFrame(results)
        con.execute("CREATE OR REPLACE TABLE backtest_results AS SELECT * FROM results_df")
        con.close()
        print("[OK] Wyniki backtestu zapisane do DuckDB")

    run_backtest()
