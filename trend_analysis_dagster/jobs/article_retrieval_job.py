# trend_analysis_dagster/jobs/article_retrieval_job.py
from dagster import job
from trend_analysis_dagster.ops.retrieve_articles_ops import (
    collect_daily_news
)

@job(
    description="Job to retrieve financial news articles from various sources",
    tags={"type": "data_ingestion"}
)
def article_retrieval_job():
    """Job that retrieves financial news articles and stores them in Firestore."""
    collect_daily_news()