from dagster import job
from ops.trend_analysis_ops import run_monthly_trend_analysis

@job
def monthly_trend_job():
    """Job to run monthly trend analysis."""
    run_monthly_trend_analysis() 