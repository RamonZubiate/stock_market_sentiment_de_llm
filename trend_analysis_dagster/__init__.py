"""
Stock Market Sentiment Analysis using LLMs
"""

__version__ = "0.1.0"

# Import and expose the Dagster definitions
from trend_analysis_dagster.run import defs

# This makes the definitions available at the module level
__all__ = ["defs"]