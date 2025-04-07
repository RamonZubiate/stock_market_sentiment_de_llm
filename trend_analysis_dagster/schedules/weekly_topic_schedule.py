from dagster import schedule
from jobs.weekly_topic_job import weekly_topic_job

@schedule(
    job=weekly_topic_job,
    cron_schedule="0 0 * * 0",  # Run at midnight every Sunday
    execution_timezone="UTC"
)
def weekly_topic_schedule():
    """Schedule to run weekly topic analysis every Sunday."""
    return {} 