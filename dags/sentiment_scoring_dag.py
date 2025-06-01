from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
import pandas as pd
import duckdb

# Argumenty DAG-a
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Tworzenie instancji Elasticsearch (lokalnie)
es = Elasticsearch("http://localhost:9200")

with DAG(
    dag_id="sentiment_scoring_dag",
    description="Agreguje sentyment dla tickerów na podstawie news_processed",
    start_date=datetime(2025, 5, 28),
    schedule="5 * * * *",
    catchup=False,
    default_args=default_args,
    tags=["AI Buffett", "sentiment"]
) as dag:

    @task(task_id="score_sentiment")
    def score_sentiment():
        # Pobierz max 1000 newsów z ostatnich 24h
        query = {
            "size": 1000,
            "query": {
                "range": {
                    "analyzed_at": {
                        "gte": "now-24h"
                    }
                }
            }
        }

        result = es.search(index="news_processed", body=query)
        if not result["hits"]["hits"]:
            raise ValueError("Brak dokumentów z indeksu news_processed")

        rows = [hit["_source"] for hit in result["hits"]["hits"]]
        df = pd.DataFrame(rows)

        if df.empty:
            raise ValueError("Brak danych w DataFrame po zapytaniu Elasticsearch")

        # Sprawdzenie kolumn i przygotowanie agregacji
        cols = df.columns.tolist()
        agg_dict = {}

        if "finbert_score" in cols:
            agg_dict["finbert_score"] = "mean"

        if "vader" in cols:
            vader_df = df["vader"].apply(pd.Series)
            df = pd.concat([df.drop("vader", axis=1), vader_df], axis=1)
            if "compound" in df.columns:
                agg_dict["compound"] = "mean"

        if not agg_dict:
            raise ValueError("Brak obsługiwanych kolumn do agregacji")

        agg = df.groupby("source").agg(agg_dict).reset_index()

        # Zapis do DuckDB
        con = duckdb.connect("/home/ubuntu/data/market.duckdb")
        con.execute("CREATE OR REPLACE TABLE sentiment_scores AS SELECT * FROM agg")
        con.close()

        print("[OK] Zapisano sentiment_scores do DuckDB")

    score_sentiment()
