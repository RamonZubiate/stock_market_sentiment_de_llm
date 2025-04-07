from dagster import schedule
from jobs.monthly_trend_job import monthly_trend_job

@schedule(
    job=monthly_trend_job,
    cron_schedule="0 0 1 * *",  # Run at midnight UTC on the first day of every month
    execution_timezone="UTC"
)
def monthly_trend_schedule():
    """Schedule to run monthly trend analysis at midnight UTC on the first day of each month."""
    return {} 