"""
Jobs for trend analysis, article retrieval, and topic management.
"""

from .daily_trend_job import daily_trend_job
from .weekly_trend_job import weekly_trend_job
from .monthly_trend_job import monthly_trend_job
from .article_retrieval_job import article_retrieval_job
from .weekly_topic_job import weekly_topic_job

jobs = [
    daily_trend_job,
    weekly_trend_job,
    monthly_trend_job,
    article_retrieval_job,
    weekly_topic_job
]
