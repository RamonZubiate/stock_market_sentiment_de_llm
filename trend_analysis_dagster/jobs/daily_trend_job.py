from dagster import job
from ops.trend_analysis_ops import run_daily_trend_analysis

@job
def daily_trend_job():
    """Job to run daily trend analysis."""
    run_daily_trend_analysis() 