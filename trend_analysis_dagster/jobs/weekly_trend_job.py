from dagster import job
from ops.trend_analysis_ops import run_weekly_trend_analysis

@job
def weekly_trend_job():
    """Job to run weekly trend analysis."""
    run_weekly_trend_analysis() 