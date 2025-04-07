# trend_analysis_dagster/resources/__init__.py
from trend_analysis_dagster.resources.firestore import firestore_resource
from trend_analysis_dagster.resources.openai_api import openai_resource

__all__ = ["firestore_resource", "openai_resource", "finbert_resource", "news_api_resource",]

"""
Resources for external services and models.
"""

