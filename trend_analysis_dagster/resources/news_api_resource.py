from dagster import resource
import os

@resource
def news_api_resource(context):
    """Dagster resource for NewsAPI."""
    api_key = os.getenv('NEWS_API_KEY')
    if not api_key:
        raise ValueError("NEWS_API_KEY environment variable not set")
    return api_key 