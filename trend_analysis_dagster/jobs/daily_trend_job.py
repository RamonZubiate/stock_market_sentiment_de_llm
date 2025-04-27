from dagster import graph
from trend_analysis_dagster.ops.trend_analysis_ops import get_todays_clusters, analyze_daily_trends_with_web_search, store_daily_analysis
from trend_analysis_dagster.resources.firestore import firestore_resource
from trend_analysis_dagster.resources.openai_api import openai_resource

@graph
def daily_trend_graph():
    clusters = get_todays_clusters()
    analysis = analyze_daily_trends_with_web_search(daily_clusters=clusters)
    store_daily_analysis(daily_clusters=clusters, daily_analysis=analysis)

daily_trend_job = daily_trend_graph.to_job(
    description="Job to run daily trend analysis.",
    resource_defs={
        "firestore": firestore_resource,
        "openai": openai_resource
    }
)
