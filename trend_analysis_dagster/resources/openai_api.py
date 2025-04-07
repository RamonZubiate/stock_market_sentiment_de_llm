# trend_analysis_dagster/resources/openai_api.py
import os
import logging
import openai
import json
from dagster import resource, Field, String

# Setup logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class OpenAIResource:
    def __init__(self, api_key=None):
        """
        Initialize the OpenAI API client.
        
        Args:
            api_key: The OpenAI API key. If None, will try to load from environment variables.
        """
        self.api_key = api_key or os.getenv('OPENAI_API_KEY')
        if not self.api_key:
            logger.warning("No OpenAI API key provided. Set OPENAI_API_KEY in your .env file.")
        openai.api_key = self.api_key
    
    def get_api_key(self):
        """
        Get the OpenAI API key.
        
        Returns:
            The OpenAI API key.
        """
        return self.api_key
        
    def generate_completion(self, model, messages, web_search=False, search_context_size="low"):
        """
        Generate a completion using the OpenAI API.
        
        Args:
            model: The OpenAI model to use.
            messages: The messages to pass to the API.
            web_search: Whether to enable web search.
            search_context_size: The size of the search context.
            
        Returns:
            The API response.
        """
        try:
            if web_search and "search" in model:
                logger.info(f"Calling OpenAI API with web search (model: {model})")
                response = openai.ChatCompletion.create(
                    model=model,
                    web_search_options={"search_context_size": search_context_size},
                    messages=messages
                )
            else:
                logger.info(f"Calling OpenAI API (model: {model})")
                response = openai.ChatCompletion.create(
                    model=model,
                    messages=messages
                )
            
            return response
        except Exception as e:
            logger.error(f"Error calling OpenAI API: {e}")
            raise e
            
    def extract_json_from_response(self, response):
        """
        Extract JSON content from an OpenAI API response.
        
        Args:
            response: The OpenAI API response.
            
        Returns:
            The extracted JSON content.
        """
        try:
            content = response.choices[0].message.content
            logger.info(f"Received response: {content[:100]}...")

            json_start = content.find('{')
            json_end = content.rfind('}') + 1
            
            if json_start >= 0 and json_end > json_start:
                json_content = content[json_start:json_end]
                return json.loads(json_content)
            else:
                logger.error("No valid JSON found in the API response.")
                return {"error": "Invalid JSON response"}
        except Exception as e:
            logger.error(f"Error extracting JSON from API response: {e}")
            return {"error": str(e)}

@resource({
    "api_key": Field(String, description="OpenAI API key", is_required=False),
})
def openai_resource(init_context):
    """
    A Dagster resource that provides access to the OpenAI API.
    
    This resource initializes the OpenAI API client and returns an OpenAIResource
    that can be used to generate completions.
    """
    return OpenAIResource(init_context.resource_config.get("api_key"))