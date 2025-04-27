# trend_analysis_dagster/jobs/__init__.py
"""
Jobs for trend analysis, article retrieval, topic management, and cluster analysis.
"""

from .daily_trend_job import daily_trend_job
from .weekly_trend_job import weekly_trend_job
from .monthly_trend_job import monthly_trend_job
from .article_retrieval_job import article_retrieval_job
from .weekly_topic_job import weekly_topic_job
from .daily_cluster_job import daily_cluster_job
from .weekly_cluster_job import weekly_cluster_job
from .monthly_cluster_job import monthly_cluster_job

jobs = [
    # Trend analysis jobs
    daily_trend_job,
    weekly_trend_job,
    monthly_trend_job,
    
    # Article retrieval job
    article_retrieval_job,
    
    # Topic management job
    weekly_topic_job,
    
    # Cluster analysis jobs
    daily_cluster_job,
    weekly_cluster_job,
    monthly_cluster_job
]