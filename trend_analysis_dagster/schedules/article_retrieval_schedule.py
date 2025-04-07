from dagster import schedule
from jobs.article_retrieval_job import article_retrieval_job

@schedule(
    job=article_retrieval_job,
    cron_schedule="0 18 * * *",  # Run at 6 PM CST (18:00) every day
    execution_timezone="America/Chicago"  # CST timezone
)
def article_retrieval_schedule():
    """Schedule to run article retrieval at 6 PM CST (7 PM EST) every day."""
    return {} 