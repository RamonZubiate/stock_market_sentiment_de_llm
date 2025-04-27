# trend_analysis_dagster/jobs/weekly_cluster_job.py
from dagster import job
from trend_analysis_dagster.ops.cluster_ops import get_weekly_clustered_trends
from trend_analysis_dagster.resources.firestore import firestore_resource
from trend_analysis_dagster.resources.openai_api import openai_resource
from trend_analysis_dagster.resources.finbert_resource import finbert_resource
@job(
    description="Job to cluster articles into weekly trends",
    tags={"type": "clustering"},
    resource_defs={
        "firestore": firestore_resource,
        "openai": openai_resource,
        "finbert": finbert_resource
    },
    config={
        "ops": {
            "get_weekly_clustered_trends": {
                "config": {
                    "method": "kmeans",
                    "n_clusters": 15,
                    "auto_clusters": True
                }
            }
        }
    }
)
def weekly_cluster_job():
    """Job to identify article clusters for weekly trends."""
    get_weekly_clustered_trends()