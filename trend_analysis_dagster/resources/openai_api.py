import os
import logging
from openai import OpenAI
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
        self.client = OpenAI(api_key=self.api_key)
    
    def get_api_key(self):
        """
        Get the OpenAI API key.
        
        Returns:
            The OpenAI API key.
        """
        return self.api_key
        
    def generate_completion(self, model, web_search_options, messages):
        """
        Generate a completion using the OpenAI API.
        
        Args:
            model: The OpenAI model to use (e.g., "gpt-4").
            messages: A list of messages in the chat conversation format.
            
        Returns:
            The API response.
        """
        try:
            if web_search_options:
                response = self.client.chat.completions.create(
                model=model,
                web_search_options=web_search_options,
                messages=messages
            )
            else:
                logger.info(f"Calling OpenAI API with model {model}")
                response = self.client.chat.completions.create(
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
            # Handling the response for ChatCompletion model
            if hasattr(response, 'choices') and len(response.choices) > 0:
                content = response.choices[0].message['content']
                logger.info(f"Received response: {content[:100]}...")

                # Extract JSON if present
                json_start = content.find('{')
                json_end = content.rfind('}') + 1
                if json_start >= 0 and json_end > json_start:
                    json_content = content[json_start:json_end]
                    return json.loads(json_content)
                else:
                    logger.error("No valid JSON found in the API response.")
                    return {"error": "Invalid JSON response"}
            else:
                logger.error("API response does not have valid choices.")
                return {"error": "Invalid API response"}
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
