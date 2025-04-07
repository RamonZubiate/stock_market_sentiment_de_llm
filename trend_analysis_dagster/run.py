from dagster import Definitions, EnvVar, ConfigurableResource
import os
import firebase_admin
from firebase_admin import credentials, firestore
import openai
import requests
from transformers import AutoModelForSequenceClassification, AutoTokenizer
import torch
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get the absolute path to the credentials file
CREDS_PATH = os.path.join(os.path.dirname(__file__), "market-sentiment-de-llm-firebase-adminsdk-fbsvc-9b39350acd.json")

class FirestoreResource(ConfigurableResource):
    """Resource for Firestore database access."""
    
    def setup_resource(self, _):
        try:
            cred = credentials.Certificate(CREDS_PATH)
            if not firebase_admin._apps:  # Only initialize if not already initialized
                firebase_admin.initialize_app(cred)
            return firestore.client()
        except Exception as e:
            raise Exception(f"Failed to initialize Firestore with credentials from {CREDS_PATH}: {str(e)}")

class OpenAIResource(ConfigurableResource):
    """Resource for OpenAI API access."""
    api_key: str = EnvVar("OPENAI_API_KEY")
    
    def setup_resource(self, _):
        openai.api_key = self.api_key
        return openai

class NewsAPIResource(ConfigurableResource):
    """Resource for NewsAPI access."""
    api_key: str = EnvVar("NEWS_API_KEY")
    
    def setup_resource(self, _):
        return self.api_key

class FinBERTResource(ConfigurableResource):
    """Resource for FinBERT sentiment analysis."""
    model_name: str = "ProsusAI/finbert"
    
    def setup_resource(self, _):
        # Load model and tokenizer
        model = AutoModelForSequenceClassification.from_pretrained(self.model_name)
        tokenizer = AutoTokenizer.from_pretrained(self.model_name)
        
        # Move model to GPU if available
        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        model = model.to(device)
        
        return {
            "model": model,
            "tokenizer": tokenizer,
            "device": device,
            "labels": ["positive", "negative", "neutral"]
        }

# Import jobs and schedules
from jobs import jobs
from schedules import schedules

# Create Dagster definitions with resources
defs = Definitions(
    jobs=jobs,
    schedules=schedules,
    resources={
        "firestore": FirestoreResource(),
        "openai": OpenAIResource(),
        "news_api_key": NewsAPIResource(),
        "finbert": FinBERTResource(),
    }
)

if __name__ == "__main__":
    # Run the Dagster daemon
    from dagster import DagsterInstance
    from dagster._core.launcher import DefaultRunLauncher
    
    instance = DagsterInstance.get()
    run_launcher = DefaultRunLauncher()
    
    # Start the Dagster daemon
    instance.run_launcher = run_launcher
    instance.run_launcher.start()
