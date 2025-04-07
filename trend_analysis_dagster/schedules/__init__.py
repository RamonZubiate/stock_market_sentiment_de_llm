"""
Schedules for trend analysis, article retrieval, and topic management.
"""

from .daily_trend_schedule import daily_trend_schedule
from .weekly_trend_schedule import weekly_trend_schedule
from .monthly_trend_schedule import monthly_trend_schedule
from .article_retrieval_schedule import article_retrieval_schedule
from .weekly_topic_schedule import weekly_topic_schedule

schedules = [
    daily_trend_schedule,
    weekly_trend_schedule,
    monthly_trend_schedule,
    article_retrieval_schedule,
    weekly_topic_schedule
]
