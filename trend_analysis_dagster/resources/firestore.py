# trend_analysis_dagster/resources/firestore.py
import os
import logging
import firebase_admin
from firebase_admin import credentials, firestore
from dagster import resource, Field, String

# Setup logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FirestoreResource:
    def __init__(self, credentials_path=None):
        """
        Initialize the Firestore resource.
        
        Args:
            credentials_path: Path to the Firebase credentials JSON file.
                             If None, will try to load from environment variables.
        """
        self.credentials_path = credentials_path or os.getenv(
            'FIREBASE_CREDENTIALS_PATH', 
            'trend_analysis_dagster/market-sentiment-de-llm-firebase-adminsdk-fbsvc-9b39350acd.json'
        )
    
    def get_db(self):
        """
        Initialize and return a Firestore database client.
        
        Returns:
            A Firestore client instance.
        """
        if not firebase_admin._apps:
            try:
                cred = credentials.Certificate(self.credentials_path)
                firebase_admin.initialize_app(cred)
                logger.info("Firebase Admin SDK initialized successfully")
            except Exception as e:
                logger.error(f"Error initializing Firebase Admin SDK: {e}")
                raise
                
        return firestore.client()

@resource({
    "credentials_path": Field(
        String, 
        description="Path to Firebase credentials JSON file", 
        is_required=False
    ),
})
def firestore_resource(init_context):
    """
    A Dagster resource that provides access to a Firestore database.
    
    This resource initializes the Firebase Admin SDK and returns a FirestoreResource
    that can be used to get a Firestore client.
    """
    return FirestoreResource(init_context.resource_config.get("credentials_path"))