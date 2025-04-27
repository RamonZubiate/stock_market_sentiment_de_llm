from dagster import graph
from trend_analysis_dagster.ops.trend_analysis_ops import get_weeks_clusters, analyze_weekly_trends_with_web_search, store_weekly_analysis
from trend_analysis_dagster.resources.firestore import firestore_resource
from trend_analysis_dagster.resources.openai_api import openai_resource

@graph
def weekly_trend_graph():
    clusters = get_weeks_clusters()
    analysis = analyze_weekly_trends_with_web_search(weekly_clusters=clusters)
    store_weekly_analysis(weekly_clusters=clusters, weekly_analysis=analysis)

weekly_trend_job = weekly_trend_graph.to_job(
    description="Job to run weekly trend analysis.",
    resource_defs={
        "firestore": firestore_resource,
        "openai": openai_resource
    }
)
