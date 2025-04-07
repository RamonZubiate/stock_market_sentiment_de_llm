from dagster import schedule
from jobs.weekly_trend_job import weekly_trend_job

@schedule(
    job=weekly_trend_job,
    cron_schedule="0 0 * * 0",  # Run at midnight UTC every Sunday
    execution_timezone="UTC"
)
def weekly_trend_schedule():
    """Schedule to run weekly trend analysis at midnight UTC on Sundays."""
    return {} 