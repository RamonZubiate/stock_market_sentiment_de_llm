# trend_analysis_dagster/jobs/article_retrieval_job.py
from dagster import job
from trend_analysis_dagster.ops.retrieve_articles_ops import collect_daily_news
from trend_analysis_dagster.resources.firestore import firestore_resource
from trend_analysis_dagster.resources.news_api_resource import news_api_resource

@job(
    description="Job to retrieve financial news articles from various sources",
    tags={"type": "data_ingestion"},
    resource_defs={
        "firestore": firestore_resource,
        "news_api_key": news_api_resource
    }
)
def article_retrieval_job():
    """Job that retrieves financial news articles and stores them in Firestore."""
    collect_daily_news()