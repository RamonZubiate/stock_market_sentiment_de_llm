from dagster import job
from ops.topics_ops import run_weekly_topic_management

@job
def weekly_topic_job():
    """
    Job to run the weekly topic management operations.
    This job orchestrates the execution of the topic management operations defined in the ops module.
    """
    run_weekly_topic_management()