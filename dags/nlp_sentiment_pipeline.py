from airflow.decorators import dag, task
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from transformers import pipeline

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="nlp_sentiment_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 5, 1),
    schedule="30 * * * *",  # co godzinę
    catchup=False,
    tags=["nlp", "sentiment"]
)
def nlp_sentiment():

    @task()
    def fetch_news():
        es = Elasticsearch("http://localhost:9200")
        res = es.search(index="news_raw", size=10)
        return [hit["_source"] for hit in res["hits"]["hits"]]

    @task()
    def analyze_sentiment(news_items):
        analyzer = SentimentIntensityAnalyzer()

        # STABILNY publiczny model
        finbert = pipeline(
            "sentiment-analysis",
            model="ProsusAI/finbert"
        )

        enriched = []
        for item in news_items:
            text = item.get("title", "") + " " + item.get("summary", "")
            vader_score = analyzer.polarity_scores(text)
            finbert_result = finbert(text[:512])[0]

            item["vader"] = vader_score
            item["finbert_label"] = finbert_result["label"]
            item["finbert_score"] = finbert_result["score"]
            item["analyzed_at"] = datetime.utcnow().isoformat()
            enriched.append(item)

        return enriched

    @task()
    def save_processed(news_items):
        es = Elasticsearch("http://localhost:9200")
        for item in news_items:
            es.index(index="news_processed", document=item)

    raw = fetch_news()
    processed = analyze_sentiment(raw)
    save_processed(processed)

nlp_sentiment()
