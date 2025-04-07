import openai
import os
import json
import logging
import datetime
import traceback
from firebase_admin import firestore
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

MAX_TOPICS = 20  # Maximum allowed number of topics in the database

class TopicManager:
    def __init__(self, db, api_key=None):
        """
        Initialize the topic manager.
        
        Args:
            db: Firestore client instance
            api_key: OpenAI API key (if None, will try to get from environment)
        """
        self.db = db
        self.api_key = api_key or os.getenv('OPENAI_API_KEY')
        if not self.api_key:
            logger.warning("No OpenAI API key provided. Set OPENAI_API_KEY in your .env file.")
        openai.api_key = self.api_key
        
    def get_current_topics(self):
        """
        Get all topics from the database with their metrics.
        
        Returns:
            List of topic dictionaries
        """
        topics_ref = self.db.collection('topics')
        topics = []
        
        try:
            for doc in topics_ref.stream():
                topic_data = doc.to_dict()
                topic_data['id'] = doc.id  # Add document ID for reference
                topics.append(topic_data)
            
            return topics
        except Exception as e:
            logger.error(f"Error getting topics: {e}")
            return []

    # -------------------------------------------------------------------------
    # WEEKLY CLUSTERS
    # -------------------------------------------------------------------------
    def get_weekly_clusters(self):
        """
        Get the current week's article clusters from the database.
        If not available, try to collect the past 7 days of daily clusters.
        
        Returns:
            List of cluster dictionaries, or empty list if none found
        """
        today = datetime.datetime.now()
        current_week = today.isocalendar()[1]
        current_year = today.year
        
        weekly_doc_id = f"weekly_{current_year}_W{current_week}"
        clusters_ref = self.db.collection('clustered_trends').document(weekly_doc_id)
        
        try:
            doc = clusters_ref.get()
            if doc.exists:
                logger.info(f"Found weekly clusters document: {weekly_doc_id}")
                clusters_data = doc.to_dict()
                return clusters_data.get('trends', [])
            else:
                logger.warning(f"No weekly clusters found for {weekly_doc_id}")
                
                # Try the previous week as fallback
                prev_date = today - datetime.timedelta(days=7)
                prev_week = prev_date.isocalendar()[1]
                prev_year = prev_date.year
                prev_weekly_doc_id = f"weekly_{prev_year}_W{prev_week}"
                prev_clusters_ref = self.db.collection('clustered_trends').document(prev_weekly_doc_id)
                
                prev_doc = prev_clusters_ref.get()
                if prev_doc.exists:
                    logger.info(f"Using previous week's clusters as fallback: {prev_weekly_doc_id}")
                    clusters_data = prev_doc.to_dict()
                    return clusters_data.get('trends', [])
                
                # If no weekly documents exist, try to collect the past 7 days of daily clusters
                logger.info("No weekly clusters found, collecting daily clusters from the past 7 days")
                return self.collect_past_week_daily_clusters()
        except Exception as e:
            logger.error(f"Error getting weekly clusters: {e}")
            logger.error(traceback.format_exc())
            return []

    def collect_past_week_daily_clusters(self):
        """
        Collect clusters from the past 7 days of daily cluster documents.
        
        Returns:
            List of combined cluster dictionaries
        """
        combined_clusters = []
        today = datetime.datetime.now()
        
        for i in range(7):
            date = today - datetime.timedelta(days=i)
            date_str = date.strftime('%Y-%m-%d')
            daily_doc_id = f"daily_{date_str}"
            
            try:
                doc = self.db.collection('clustered_trends').document(daily_doc_id).get()
                if doc.exists:
                    clusters_data = doc.to_dict()
                    daily_clusters = clusters_data.get('trends', [])
                    
                    # Add date info to each cluster
                    for cluster in daily_clusters:
                        cluster['source_date'] = date_str
                    
                    combined_clusters.extend(daily_clusters)
                    logger.info(f"Added {len(daily_clusters)} clusters from {date_str}")
            except Exception as e:
                logger.warning(f"Error getting clusters for {date_str}: {e}")
        
        logger.info(f"Collected {len(combined_clusters)} clusters from the past 7 days")
        return combined_clusters

    # -------------------------------------------------------------------------
    # MONTHLY CLUSTERS
    # -------------------------------------------------------------------------
    def get_monthly_clusters(self):
        """
        Get the current month's article clusters from the database.
        If not available, do an optional fallback or return empty list.
        
        Returns:
            List of cluster dictionaries, or empty if none found
        """
        today = datetime.datetime.now()
        current_month = today.month
        current_year = today.year
        
        monthly_doc_id = f"monthly_{current_year}_{current_month:02d}"
        clusters_ref = self.db.collection('clustered_trends').document(monthly_doc_id)
        
        try:
            doc = clusters_ref.get()
            if doc.exists:
                logger.info(f"Found monthly clusters document: {monthly_doc_id}")
                clusters_data = doc.to_dict()
                return clusters_data.get('trends', [])
            else:
                logger.warning(f"No monthly clusters found for {monthly_doc_id}")
                # Optionally fallback to the previous month
                prev_month_date = today - relativedelta(months=1)
                prev_month = prev_month_date.month
                prev_year = prev_month_date.year
                prev_monthly_doc_id = f"monthly_{prev_year}_{prev_month:02d}"
                prev_clusters_ref = self.db.collection('clustered_trends').document(prev_monthly_doc_id)
                prev_doc = prev_clusters_ref.get()
                if prev_doc.exists:
                    logger.info(f"Using previous month's clusters as fallback: {prev_monthly_doc_id}")
                    clusters_data = prev_doc.to_dict()
                    return clusters_data.get('trends', [])
                
                logger.info("No monthly clusters found even after fallback.")
                return []
        except Exception as e:
            logger.error(f"Error getting monthly clusters: {e}")
            logger.error(traceback.format_exc())
            return []

    # -------------------------------------------------------------------------
    # UTILITY
    # -------------------------------------------------------------------------
    def get_article_titles(self, article_ids, max_titles=10):
        """
        Get article titles from a list of article IDs.
        """
        titles = []
        articles_ref = self.db.collection('articles')
        
        try:
            for article_id in article_ids[:max_titles]:
                doc = articles_ref.document(article_id).get()
                if doc.exists:
                    article_data = doc.to_dict()
                    title = article_data.get('title', '')
                    if title:
                        titles.append(title)
            return titles
        except Exception as e:
            logger.error(f"Error getting article titles: {e}")
            return []
    
    # -------------------------------------------------------------------------
    # GPT ANALYSIS
    # -------------------------------------------------------------------------
    def analyze_topics_with_web_search(self, current_topics, clusters, period="weekly"):
        """
        Use ChatGPT with web search to analyze topics and recommend changes.

        Args:
            current_topics: List of topic dictionaries
            clusters: List of cluster dictionaries
            period: 'weekly' or 'monthly'
        
        Returns:
            Dictionary of actions to take
        """
        if not self.api_key:
            logger.error("No OpenAI API key available")
            return {"error": "No API key"}
        
        # Format current topics
        topics_text = "CURRENT TOPICS:\n"
        for topic in current_topics:
            name = topic.get('name', '')
            keyword = topic.get('keyword', '')
            category = topic.get('category_id', 'unknown')
            active = "active" if topic.get('active', False) else "inactive"
            topics_text += f"- {name} (keyword: '{keyword}'): status={active}, category={category}\n"

        # Format clusters
        clusters_text = f"THIS {period.upper()} CLUSTERS:\n"
        sorted_clusters = sorted(clusters, key=lambda x: x.get('article_count', 0), reverse=True)
        for i, cluster in enumerate(sorted_clusters[:20]):
            keywords = ', '.join(cluster.get('keywords', [])[:5])
            article_count = cluster.get('article_count', 0)
            clusters_text += f"Cluster {i+1}: Keywords [{keywords}] - {article_count} articles\n"
            
            article_ids = cluster.get('articles', [])
            sample_titles = self.get_article_titles(article_ids, max_titles=5)
            
            for title in sample_titles:
                clusters_text += f"* \"{title}\"\n"
            
            if 'topic' in cluster:
                clusters_text += f"Generated topic: \"{cluster['topic']}\"\n"
            clusters_text += "\n"
        
        prompt = f"""You are managing a financial news topic database. Below are our currently tracked topics with their exact keywords:

{topics_text}

{clusters_text}

Based on this {period}'s news clusters and the latest financial news from the web:
1. Which of our EXACT topic keywords (not names) are present in this {period}'s articles and recent news?
2. Which topics should be marked inactive or removed?
3. What new topics should we add from this {period}'s clusters or trending financial news?
4. Assign a sentiment to each new topic (one of: "bullish", "bearish", or "stagnant").
5. Provide an overall sentiment for this {period}'s coverage (one of: "bullish", "bearish", or "mixed") in 'overall_sentiment'.

IMPORTANT: When identifying present topics, use the EXACT keyword as it appears in our database.

FORMAT YOUR RESPONSE AS JSON:
{{
  "overall_sentiment": "bullish|bearish|mixed",
  "present_topics": ["exactkeyword1", "exactkeyword2"],
  "topics_to_deactivate": ["exactkeyword3", "exactkeyword4"],
  "new_topics": [
    {{
      "name": "new topic 1",
      "keyword": "newtopic1",
      "category": "economic|corporate|geopolitical|sentiment",
      "sentiment": "bullish|bearish|stagnant"
    }},
    {{
      "name": "new topic 2",
      "keyword": "newtopic2",
      "category": "economic|corporate|geopolitical|sentiment",
      "sentiment": "bullish|bearish|stagnant"
    }}
  ],
  "reasoning": "Brief explanation of your decisions",
  "analysis": "Comprehensive analysis of {period} trends and any external news"
}}"""

        logger.info(f"Calling ChatGPT API with web search for {period} topic analysis")
        model = "gpt-4o-search-preview"
        
        try:
            response = openai.ChatCompletion.create(
                model=model,
                web_search_options={"search_context_size": "medium"},
                messages=[
                    {
                        "role": "system", 
                        "content": "You are a financial topic management system that responds in JSON format only. Be precise and consistent in your analysis."
                    },
                    {"role": "user", "content": prompt}
                ]
            )
            
            content = response.choices[0].message.content
            logger.info(f"Received GPT response: {content[:100]}...")
            
            json_start = content.find('{')
            json_end = content.rfind('}') + 1
            if json_start >= 0 and json_end > json_start:
                return json.loads(content[json_start:json_end])
            else:
                logger.error("No valid JSON found in the response")
                return {"error": "Invalid response format"}
                
        except Exception as e:
            logger.error(f"Error calling ChatGPT API with web search: {e}")
            logger.error(traceback.format_exc())
            return {"error": str(e)}
    
    def analyze_topics_without_search(self, current_topics, clusters, period="weekly"):
        """
        Fallback method to analyze topics without web search.
        (Omitted for brevity.)
        """
        return {"error": "Fallback not implemented in this snippet"}

    # -------------------------------------------------------------------------
    # APPLYING CHANGES
    # -------------------------------------------------------------------------
    def apply_topic_changes(self, actions):
        """
        Apply recommended changes to the topics database, respecting the max limit of 20.
        Also track how often a topic is seen (seen_count) and when it was last seen (last_seen_at).
        """
        topics_ref = self.db.collection('topics')
        results = {
            "updated": 0,
            "deactivated": 0,
            "added": 0,
            "errors": 0
        }
        
        try:
            current_topics = list(topics_ref.stream())
            current_count = len(current_topics)
            logger.info(f"Current number of topics: {current_count}")

            # 1) Update present topics
            for topic_keyword in actions.get('present_topics', []):
                query = topics_ref.where('keyword', '==', topic_keyword)
                docs = list(query.stream())
                if docs:
                    topic_doc = docs[0]
                    try:
                        existing_data = topic_doc.to_dict()
                        old_seen = existing_data.get('seen_count', 0)
                        topic_doc.reference.update({
                            'active': True,
                            'seen_count': old_seen + 1,
                            'last_seen_at': firestore.SERVER_TIMESTAMP,
                            'updated_at': firestore.SERVER_TIMESTAMP
                        })
                        results["updated"] += 1
                    except Exception as e:
                        logger.error(f"Error updating topic {topic_keyword}: {e}")
                        results["errors"] += 1

            # 2) Deactivate / delete topics
            for topic_keyword in actions.get('topics_to_deactivate', []):
                query = topics_ref.where('keyword', '==', topic_keyword)
                docs = list(query.stream())
                if docs:
                    topic_doc = docs[0]
                    try:
                        topic_doc.reference.delete()
                        results["deactivated"] += 1
                    except Exception as e:
                        logger.error(f"Error deleting topic {topic_keyword}: {e}")
                        results["errors"] += 1
            
            # 3) Add new topics
            new_topics_list = actions.get('new_topics', [])
            for new_topic in new_topics_list:
                if current_count >= MAX_TOPICS:
                    logger.info("Max topic count reached; skipping more new topics.")
                    break
                
                topic_name = new_topic.get('name', '')
                topic_keyword = new_topic.get('keyword', '')
                if not topic_keyword:
                    topic_keyword = topic_name.lower().replace(' ', '')
                
                query = topics_ref.where('keyword', '==', topic_keyword)
                docs = list(query.stream())
                if docs:
                    logger.info(f"Topic with keyword '{topic_keyword}' already exists. Skipping.")
                    continue
                
                try:
                    category = new_topic.get('category', 'sentiment')
                    if 'economic' in category:
                        category = 'economic'
                    elif 'geopolitical' in category:
                        category = 'geopolitical'
                    elif 'corporate' in category:
                        category = 'corporate'
                    else:
                        category = 'sentiment'
                    
                    sentiment_value = new_topic.get('sentiment', 'stagnant')
                    
                    topics_ref.add({
                        'name': topic_name,
                        'keyword': topic_keyword,
                        'category_id': category,
                        'sentiment': sentiment_value,
                        'active': True,
                        'seen_count': 1,
                        'last_seen_at': firestore.SERVER_TIMESTAMP,
                        'created_at': firestore.SERVER_TIMESTAMP,
                        'updated_at': firestore.SERVER_TIMESTAMP,
                        'source': 'auto_gpt'
                    })
                    results["added"] += 1
                    current_count += 1
                except Exception as e:
                    logger.error(f"Error adding new topic {topic_keyword}: {e}")
                    results["errors"] += 1
            
            return results
        except Exception as e:
            logger.error(f"Error applying topic changes: {e}")
            logger.error(traceback.format_exc())
            return {"error": str(e)}
    
    # -------------------------------------------------------------------------
    # STORING THE GPT ANALYSIS IN trend_analysis
    # -------------------------------------------------------------------------
    def store_trend_analysis(self, period, actions):
        """
        Store the GPT analysis in the 'trend_analysis' collection.
        
        We assume `actions` might have fields like:
            - actions["analysis"]
            - actions["overall_sentiment"]
            - anything else we want to store
        """
        try:
            # You can store the entire GPT result or just partial. 
            # Below, we store:
            #   'analysis' (the detailed explanation)
            #   'overall_sentiment'
            #   'timestamp'
            #   'period'
            #   'raw_json' (the entire actions for reference, optional)
            
            doc_ref = self.db.collection('trend_analysis').document()
            data = {
                'period': period,
                'timestamp': firestore.SERVER_TIMESTAMP,
                'overall_sentiment': actions.get("overall_sentiment", "N/A"),
                'analysis': actions.get("analysis", ""),  # the textual analysis from GPT
                'raw_json': actions  # you can store the entire dictionary, or omit
            }
            doc_ref.set(data)
            return doc_ref.id
        except Exception as e:
            logger.error(f"Error storing trend analysis: {e}")
            logger.error(traceback.format_exc())
            return None
    
    # -------------------------------------------------------------------------
    # LOGGING
    # -------------------------------------------------------------------------
    def log_topic_management(self, actions, results, period="weekly"):
        """
        Log the topic management actions for review. 
        We also store 'period' so we know if it was a weekly or monthly run.
        """
        try:
            log_ref = self.db.collection('topic_management_logs').document()
            log_data = {
                'date': datetime.datetime.now().strftime('%Y-%m-%d'),
                'timestamp': firestore.SERVER_TIMESTAMP,
                'period': period,
                'actions': actions,
                'results': results,
            }
            log_ref.set(log_data)
            return log_ref.id
        except Exception as e:
            logger.error(f"Error logging topic management: {e}")
            return None

    # -------------------------------------------------------------------------
    # WEEKLY TOPIC MANAGEMENT
    # -------------------------------------------------------------------------
    def run_weekly_topic_management(self):
        """
        Run the weekly topic management process with web search (or fallback).
        """
        logger.info("Starting weekly topic management process with web search")
        
        # 1) Get current topics
        current_topics = self.get_current_topics()
        logger.info(f"Found {len(current_topics)} topics in the database")
        
        # 2) Get weekly clusters
        weekly_clusters = self.get_weekly_clusters()
        logger.info(f"Found {len(weekly_clusters)} clusters for this week")
        
        if not weekly_clusters:
            return {"error": "No weekly clusters available"}
        
        # 3) Analyze with GPT
        actions = self.analyze_topics_with_web_search(current_topics, weekly_clusters, period="weekly")
        if "error" in actions:
            return actions
        
        # 3a) Store the GPT analysis in 'trend_analysis' collection
        trend_id = self.store_trend_analysis(period="weekly", actions=actions)
        logger.info(f"Stored GPT analysis in trend_analysis with doc ID: {trend_id}")
        
        # 4) Apply changes to 'topics'
        results = self.apply_topic_changes(actions)
        
        # 5) Log the results in 'topic_management_logs'
        log_id = self.log_topic_management(actions, results, period="weekly")
        logger.info(f"Weekly topic management logged with ID: {log_id}")

        combined_results = {
            "actions": actions,
            "results": results,
            "trend_analysis_id": trend_id,
            "log_id": log_id,
            "timestamp": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        return combined_results

    # -------------------------------------------------------------------------
    # MONTHLY TOPIC MANAGEMENT
    # -------------------------------------------------------------------------
    def run_monthly_topic_management(self):
        """
        Run the monthly topic management process with web search (or fallback).
        Typically uses monthly_{YYYY}_{MM} doc in 'clustered_trends'.
        """
        logger.info("Starting monthly topic management process with web search")
        
        # 1) Get current topics
        current_topics = self.get_current_topics()
        logger.info(f"Found {len(current_topics)} topics in the database")
        
        # 2) Get monthly clusters
        monthly_clusters = self.get_monthly_clusters()
        logger.info(f"Found {len(monthly_clusters)} clusters for this month")
        
        if not monthly_clusters:
            return {"error": "No monthly clusters available"}
        
        # 3) Analyze with GPT
        actions = self.analyze_topics_with_web_search(current_topics, monthly_clusters, period="monthly")
        if "error" in actions:
            return actions
        
        # 3a) Store the GPT analysis in 'trend_analysis'
        trend_id = self.store_trend_analysis(period="monthly", actions=actions)
        logger.info(f"Stored GPT analysis in trend_analysis with doc ID: {trend_id}")
        
        # 4) Apply changes
        results = self.apply_topic_changes(actions)
        
        # 5) Log results
        log_id = self.log_topic_management(actions, results, period="monthly")
        logger.info(f"Monthly topic management logged with ID: {log_id}")

        combined_results = {
            "actions": actions,
            "results": results,
            "trend_analysis_id": trend_id,
            "log_id": log_id,
            "timestamp": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        return combined_results

# -------------------------------------------------------------------------
# Helper to run from command line or external code
# -------------------------------------------------------------------------
def run_weekly_topic_management(db):
    """
    High-level wrapper to run weekly topic management.
    """
    api_key = os.getenv('OPENAI_API_KEY')
    manager = TopicManager(db, api_key)
    results = manager.run_weekly_topic_management()
    _print_results(results, label="Weekly")

def run_monthly_topic_management(db):
    """
    High-level wrapper to run monthly topic management.
    """
    api_key = os.getenv('OPENAI_API_KEY')
    manager = TopicManager(db, api_key)
    results = manager.run_monthly_topic_management()
    _print_results(results, label="Monthly")

def _print_results(results, label="Weekly"):
    """
    Simple utility function to print results.
    """
    if results is None:
        print(f"Error: No results returned from {label.lower()} topic management")
    elif isinstance(results, dict) and "error" in results:
        print(f"Error: {results['error']}")
    elif isinstance(results, dict) and "results" in results:
        print(f"{label} Topic Management Results:")
        print(f"  Updated: {results['results'].get('updated', 0)}")
        print(f"  Deactivated: {results['results'].get('deactivated', 0)}")
        print(f"  Added: {results['results'].get('added', 0)}")
        print(f"  Errors: {results['results'].get('errors', 0)}")
        print(f"  Trend Analysis Doc: {results.get('trend_analysis_id', 'None')}")
        print(f"  Log ID: {results.get('log_id', 'None')}")
        print(f"  Timestamp: {results.get('timestamp', 'Unknown')}")
    else:
        print(f"Unexpected result format: {results}")


if __name__ == "__main__":
    import firebase_admin
    from firebase_admin import credentials, firestore
    
    try:
        if not firebase_admin._apps:
            firebase_creds_path = os.getenv('FIREBASE_CREDENTIALS_PATH', 'server/market-sentiment-de-llm-firebase-adminsdk-fbsvc-9b39350acd.json')
            cred = credentials.Certificate(firebase_creds_path)
            firebase_admin.initialize_app(cred)
        db = firestore.client()
        
        # Example usage: run weekly or monthly
        run_weekly_topic_management(db)
        # run_monthly_topic_management(db)
    except Exception as e:
        print(f"Error initializing or running topic management: {e}")
        traceback.print_exc()
