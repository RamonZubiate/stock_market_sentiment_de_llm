from dagster import schedule
from jobs.daily_trend_job import daily_trend_job

@schedule(
    job=daily_trend_job,
    cron_schedule="0 0 * * *",  # Run at midnight UTC every day
    execution_timezone="UTC"
)
def daily_trend_schedule():
    """Schedule to run daily trend analysis at midnight UTC."""
    return {} 