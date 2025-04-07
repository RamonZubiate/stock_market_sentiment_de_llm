from dagster import repository
from jobs.daily_trend_job import daily_trend_job
from jobs.weekly_trend_job import weekly_trend_job
from jobs.monthly_trend_job import monthly_trend_job
from jobs.article_retrieval_job import article_retrieval_job
from jobs.weekly_topic_job import weekly_topic_job
from schedules.daily_trend_schedule import daily_trend_schedule
from schedules.weekly_trend_schedule import weekly_trend_schedule
from schedules.monthly_trend_schedule import monthly_trend_schedule
from schedules.article_retrieval_schedule import article_retrieval_schedule
from schedules.weekly_topic_schedule import weekly_topic_schedule
from ops.trend_analysis_ops import (
    get_todays_clusters,
    analyze_daily_trends_with_web_search,
    get_article_titles,
    store_daily_analysis,
    run_daily_trend_analysis,
    get_weeks_clusters,
    analyze_weekly_trends_with_web_search,
    store_weekly_analysis,
    run_weekly_trend_analysis,
    get_monthly_clusters,
    analyze_monthly_trends_with_web_search,
    store_monthly_analysis,
    run_monthly_trend_analysis
)
from ops.topics_ops import (
    get_current_topics,
    get_weekly_clusters,
    collect_past_week_daily_clusters,
    analyze_topics_with_web_search,
    analyze_topics_without_web_search,
    apply_topic_changes,
    store_topic_analysis,
    log_topic_management,
    run_weekly_topic_management
)
from ops.retrieve_articles_ops import (
    get_topics_for_retrieval,
    fetch_articles_for_topic,
    store_articles,
    run_article_retrieval
)

@repository
def trend_analysis_repository():
    """Repository containing all jobs, schedules, and ops for trend analysis."""
    return [
        # Jobs
        daily_trend_job,
        weekly_trend_job,
        monthly_trend_job,
        article_retrieval_job,
        weekly_topic_job,
        
        # Schedules
        daily_trend_schedule,
        weekly_trend_schedule,
        monthly_trend_schedule,
        article_retrieval_schedule,
        weekly_topic_schedule,
        
        # Individual ops for testing and debugging
        # Daily trend ops
        get_todays_clusters,
        analyze_daily_trends_with_web_search,
        get_article_titles,
        store_daily_analysis,
        run_daily_trend_analysis,
        
        # Weekly trend ops
        get_weeks_clusters,
        analyze_weekly_trends_with_web_search,
        store_weekly_analysis,
        run_weekly_trend_analysis,
        
        # Monthly trend ops
        get_monthly_clusters,
        analyze_monthly_trends_with_web_search,
        store_monthly_analysis,
        run_monthly_trend_analysis,
        
        # Topic management ops
        get_current_topics,
        get_weekly_clusters,
        collect_past_week_daily_clusters,
        analyze_topics_with_web_search,
        analyze_topics_without_web_search,
        apply_topic_changes,
        store_topic_analysis,
        log_topic_management,
        run_weekly_topic_management,
        
        # Article retrieval ops
        get_topics_for_retrieval,
        fetch_articles_for_topic,
        store_articles,
        run_article_retrieval
    ]