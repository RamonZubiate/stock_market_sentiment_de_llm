from dagster import graph
from trend_analysis_dagster.ops.trend_analysis_ops import get_monthly_clusters, analyze_monthly_trends_with_web_search, store_monthly_analysis
from trend_analysis_dagster.resources.firestore import firestore_resource
from trend_analysis_dagster.resources.openai_api import openai_resource

@graph
def monthly_trend_graph():
    clusters = get_monthly_clusters()
    analysis = analyze_monthly_trends_with_web_search(monthly_clusters=clusters)
    store_monthly_analysis(monthly_clusters=clusters, monthly_analysis=analysis)

monthly_trend_job = monthly_trend_graph.to_job(
    description="Job to run monthly trend analysis.",
    resource_defs={
        "firestore": firestore_resource,
        "openai": openai_resource
    }
)
