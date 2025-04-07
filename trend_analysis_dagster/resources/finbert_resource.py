from dagster import resource
from transformers import AutoModelForSequenceClassification, AutoTokenizer
import torch
import logging

logger = logging.getLogger(__name__)

@resource
def finbert_resource(context):
    """Dagster resource for FinBERT model."""
    try:
        sentiment_tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert")
        sentiment_model = AutoModelForSequenceClassification.from_pretrained("ProsusAI/finbert")
        sentiment_model.eval()  # Set to evaluation mode
        
        return {
            "sentiment_tokenizer": sentiment_tokenizer,
            "sentiment_model": sentiment_model,
            "finbert_available": True
        }
    except Exception as e:
        logger.warning(f"FinBERT model loading failed: {e}. Will use basic sentiment analysis.")
        return {
            "sentiment_tokenizer": None,
            "sentiment_model": None,
            "finbert_available": False
        } 