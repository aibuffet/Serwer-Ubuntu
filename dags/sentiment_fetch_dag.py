from airflow.decorators import dag, task
from datetime import datetime, timedelta
import requests
import feedparser
from elasticsearch import Elasticsearch

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="sentiment_fetch_dag",
    default_args=default_args,
    start_date=datetime(2025, 5, 1),
    schedule="0 * * * *",
    catchup=False,
    tags=["sentiment", "rss", "reddit", "crypto"]
)
def sentiment_fetch():

    @task()
    def fetch_rss():
        topics = [
            "https://news.google.com/rss/search?q=Nasdaq+100",
            "https://news.google.com/rss/search?q=inflacja+USA",
            "https://news.google.com/rss/search?q=bitcoin",
            "https://news.google.com/rss/search?q=fed+rezerwa",
            "https://news.google.com/rss/search?q=rynek+akcji",
        ]
        articles = []
        for url in topics:
            feed = feedparser.parse(url)
            for entry in feed.entries[:5]:
                articles.append({
                    "source": "google_rss",
                    "title": entry.title,
                    "published": entry.published,
                    "summary": entry.get("summary", ""),
                    "link": entry.link
                })
        return articles

    @task()
    def fetch_cryptopanic():
        url = "https://cryptopanic.com/api/v1/posts/?auth_token=41ac9722ea77f790de219a09668947c6712c385b&public=true"
        res = requests.get(url)
        data = res.json()
        return [
            {
                "source": "cryptopanic",
                "title": x["title"],
                "published": x["published_at"],
                "summary": x["slug"],
                "link": x["url"]
            }
            for x in data.get("results", [])[:10]
        ]

    @task()
    def fetch_reddit():
        headers = {"User-Agent": "ai-buffett-agent"}
        subreddits = ["stocks", "investing", "geopolitics", "cryptocurrency"]
        posts = []
        for sub in subreddits:
            url = f"https://www.reddit.com/r/{sub}/hot.json?limit=5"
            res = requests.get(url, headers=headers)
            if res.status_code == 200:
                data = res.json()
                for post in data["data"]["children"]:
                    p = post["data"]
                    posts.append({
                        "source": "reddit",
                        "title": p["title"],
                        "published": datetime.utcfromtimestamp(p["created_utc"]).isoformat(),
                        "summary": p.get("selftext", "")[:300],
                        "link": f"https://reddit.com{p['permalink']}"
                    })
        return posts

    @task()
    def combine_news(g, c, r):
        return g + c + r

    @task()
    def save_to_elasticsearch(all_news: list):
        es = Elasticsearch("http://localhost:9200")
        for article in all_news:
            es.index(index="news_raw", document=article)

    google = fetch_rss()
    crypto = fetch_cryptopanic()
    reddit = fetch_reddit()
    combined = combine_news(google, crypto, reddit)
    save_to_elasticsearch(combined)

sentiment_fetch()
