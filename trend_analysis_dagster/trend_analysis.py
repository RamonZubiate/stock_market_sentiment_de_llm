#trend_analysis.py
# script to analyze daily, weekly, and monthly trends using OpenAI's GPT-4 model
# and store results in Firestore
# calls other modules for clustering and topic analysis

import openai
import os
import json
import logging
import datetime
import traceback
from firebase_admin import firestore
from dotenv import load_dotenv
import topic_analysis_update
import cluster_trends

# Load environment variables from .env file
load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DailyTrendManager:
    def __init__(self, db, api_key=None):
        """
        Initialize the daily/weekly/monthly trends manager.
        
        Args:
            db: Firestore client instance
            api_key: OpenAI API key (if None, will try to get from environment)
        """
        self.db = db
        self.api_key = api_key or os.getenv('OPENAI_API_KEY')
        if not self.api_key:
            logger.warning("No OpenAI API key provided. Set OPENAI_API_KEY in your .env file.")
        openai.api_key = self.api_key

    # -------------------------------------------------------------------------
    # DAILY CLUSTERS
    # -------------------------------------------------------------------------
    def get_todays_clusters(self, date_override=None):
        """
        Fetch clusters for today's date (or an override date) from Firestore.
        
        Args:
            date_override: A string in YYYY-MM-DD format to override "today."
        
        Returns:
            List of cluster dictionaries for the given date.
        """
        if date_override:
            target_date = date_override
        else:
            target_date = datetime.datetime.now().strftime('%Y-%m-%d')
        
        daily_doc_id = f"daily_{target_date}"
        logger.info(f"Fetching daily clusters document: {daily_doc_id}")

        clusters_ref = self.db.collection('clustered_trends').document(daily_doc_id)
        
        try:
            doc = clusters_ref.get()
            if doc.exists:
                data = doc.to_dict()
                clusters = data.get('trends', [])
                logger.info(f"Found {len(clusters)} clusters for {target_date}")
                return clusters
            else:
                logger.warning(f"No daily clusters found for {target_date}")
                return []
        except Exception as e:
            logger.error(f"Error fetching today's clusters: {e}")
            logger.error(traceback.format_exc())
            return []

    def analyze_daily_trends_with_web_search(self, daily_clusters):
        """
        Use ChatGPT (with optional web search) to provide daily market insights.
        """
        if not self.api_key:
            logger.error("No OpenAI API key available")
            return {"error": "No API key"}

        # Format daily clusters for the prompt
        clusters_text = "TODAY'S ARTICLE CLUSTERS:\n"
        sorted_clusters = sorted(daily_clusters, key=lambda x: x.get('article_count', 0), reverse=True)
        for i, cluster in enumerate(sorted_clusters[:10]):  # limit to top 10 for brevity
            keywords = ', '.join(cluster.get('keywords', [])[:5])
            article_count = cluster.get('article_count', 0)
            clusters_text += f"Cluster {i+1}: Keywords [{keywords}] - {article_count} articles\n"
            
            article_ids = cluster.get('articles', [])
            sample_titles = self.get_article_titles(article_ids, max_titles=3)
            for title in sample_titles:
                clusters_text += f"    * {title}\n"
            
            if 'topic' in cluster:
                clusters_text += f"    Generated topic: \"{cluster['topic']}\"\n"
            
            clusters_text += "\n"

        # Build the user prompt
        prompt = f"""
You are a daily financial news analyst. Your job is to summarize how the markets performed *today* based on the clusters of articles below and current financial news from the web.

{clusters_text}

Please answer:
1. Did the market go up, down, or remain flat overall? (Call it "market_move")
2. Give a brief explanation of why it moved that way (e.g., "it dipped because of poor retail earnings" or "it ripped higher on optimism over Fed rate cuts").
3. Include any other interesting daily highlights (major gainers, losers, notable events).
4. Provide your final insights in JSON format, like:
{{
  "date": "YYYY-MM-DD",
  "market_move": "up|down|flat",
  "brief_explanation": "Short reason describing why it moved.",
  "highlights": ["CompanyX soared 5%", "Oil prices sank 2%", ...],
  "reasoning": "More detailed explanation or context behind the move."
}}

IMPORTANT: Provide ONLY valid JSON as your final output.
        """

        logger.info("Calling ChatGPT for daily trend analysis with web search")
        
        try:
            # Attempt with web search
            response = openai.ChatCompletion.create(
                model="gpt-4o-search-preview",
                web_search_options={"search_context_size": "low"},
                messages=[
                    {"role": "system", "content": "You are a daily market analyst that responds in valid JSON only."},
                    {"role": "user", "content": prompt}
                ]
            )
            
            # Fallback if needed
            response = openai.ChatCompletion.create(
                model="gpt-4o-search-preview",
                messages=[
                    {"role": "system", "content": "You are a daily market analyst that responds in valid JSON only."},
                    {"role": "user", "content": prompt}
                ]
            )
            
            content = response.choices[0].message.content
            logger.info(f"Received daily analysis: {content[:100]}...")

            json_start = content.find('{')
            json_end = content.rfind('}') + 1
            if json_start >= 0 and json_end > json_start:
                json_content = content[json_start:json_end]
                return json.loads(json_content)
            else:
                logger.error("No valid JSON found in the GPT daily analysis response.")
                return {"error": "Invalid JSON response"}
        except Exception as e:
            logger.error(f"Error calling ChatGPT for daily analysis: {e}")
            logger.error(traceback.format_exc())
            return {"error": str(e)}

    def run_daily_trend_analysis(self, date_override=None):
        """
        Retrieve today's daily clusters, generate a GPT summary, and return it.
        """
        logger.info("Starting daily trend analysis")
        
        daily_clusters = self.get_todays_clusters(date_override=date_override)
        if not daily_clusters:
            return {"error": "No daily clusters found for today"}

        daily_insights = self.analyze_daily_trends_with_web_search(daily_clusters)
        return daily_insights

    def get_article_titles(self, article_ids, max_titles=5):
        """
        Retrieve a few article titles for context in the GPT prompt.
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
    # WEEKLY CLUSTERS
    # -------------------------------------------------------------------------
    def get_weeks_clusters(self, date_override=None):
        """
        Fetch clusters for this week (or an override date).
        The doc ID is 'weekly_{YEAR}_W{WEEK}'.
        """
        if date_override:
            try:
                dt = datetime.datetime.strptime(date_override, '%Y-%m-%d')
            except ValueError:
                logger.warning(f"Invalid date_override format: {date_override}. Using current date.")
                dt = datetime.datetime.now()
        else:
            dt = datetime.datetime.now()
        
        year = dt.year
        week_number = dt.isocalendar()[1]
        weekly_doc_id = f"weekly_{year}_W{week_number}"
        
        logger.info(f"Fetching weekly clusters document: {weekly_doc_id}")
        clusters_ref = self.db.collection('clustered_trends').document(weekly_doc_id)
        
        try:
            doc = clusters_ref.get()
            if doc.exists:
                data = doc.to_dict()
                clusters = data.get('trends', [])
                logger.info(f"Found {len(clusters)} clusters for week {week_number} of {year}")
                return clusters
            else:
                logger.warning(f"No weekly clusters found for {weekly_doc_id}")
                return []
        except Exception as e:
            logger.error(f"Error fetching weekly clusters: {e}")
            logger.error(traceback.format_exc())
            return []

    def analyze_weekly_trends_with_web_search(self, weekly_clusters):
        """
        Use ChatGPT to provide weekly market insights.
        """
        if not self.api_key:
            logger.error("No OpenAI API key available for weekly analysis.")
            return {"error": "No API key"}

        clusters_text = "THIS WEEK'S ARTICLE CLUSTERS:\n"
        sorted_clusters = sorted(weekly_clusters, key=lambda x: x.get('article_count', 0), reverse=True)
        for i, cluster in enumerate(sorted_clusters[:10]):  # top 10
            keywords = ', '.join(cluster.get('keywords', [])[:5])
            article_count = cluster.get('article_count', 0)
            clusters_text += f"Cluster {i+1}: Keywords [{keywords}] - {article_count} articles\n"
            
            article_ids = cluster.get('articles', [])
            sample_titles = self.get_article_titles(article_ids, max_titles=3)
            for title in sample_titles:
                clusters_text += f"    * {title}\n"
            
            if 'topic' in cluster:
                clusters_text += f"    Generated topic: \"{cluster['topic']}\"\n"
            
            clusters_text += "\n"

        prompt = f"""
You are a weekly financial news analyst. Your job is to summarize how the markets performed *this week* based on the clusters of articles below and current financial news from the web.

{clusters_text}

Please answer:
1. Did the market go up, down, or stay fairly stable overall this week? (Call it "market_move")
2. Give a brief explanation of why it moved that way.
3. Include any other interesting weekly highlights (big winners, losers, notable surprises).
4. Provide your final insights in JSON format, like:
{{
  "week_of": "YYYY-WW",
  "market_move": "up|down|flat",
  "brief_explanation": "...",
  "highlights": ["CompanyA soared 8%", "Oil crashed 4%", ...],
  "reasoning": "A more detailed explanation or context behind the week's move."
}}

IMPORTANT: Provide ONLY valid JSON as your final output.
        """

        logger.info("Calling ChatGPT for weekly trend analysis")
        
        try:
            response = openai.ChatCompletion.create(
                model="gpt-4o-search-preview",
                web_search_options={"search_context_size": "medium"},
                messages=[
                    {"role": "system", "content": "You are a weekly market analyst that responds in valid JSON only."},
                    {"role": "user", "content": prompt}
                ]
            )
            
            # Fallback
            response = openai.ChatCompletion.create(
                model="gpt-4o-search-preview",
                messages=[
                    {"role": "system", "content": "You are a weekly market analyst that responds in valid JSON only."},
                    {"role": "user", "content": prompt}
                ]
            )
            
            content = response.choices[0].message.content
            logger.info(f"Received weekly analysis: {content[:100]}...")

            json_start = content.find('{')
            json_end = content.rfind('}') + 1
            if json_start >= 0 and json_end > json_start:
                return json.loads(content[json_start:json_end])
            else:
                logger.error("No valid JSON found in the GPT weekly analysis response.")
                return {"error": "Invalid JSON response"}
        except Exception as e:
            logger.error(f"Error calling ChatGPT for weekly analysis: {e}")
            logger.error(traceback.format_exc())
            return {"error": str(e)}

    def run_weekly_trend_analysis(self, date_override=None):
        """
        Retrieve this week's clusters (or override date's week),
        and generate a weekly summary using GPT.
        """
        logger.info("Starting weekly trend analysis")
        
        weekly_clusters = self.get_weeks_clusters(date_override=date_override)
        if not weekly_clusters:
            return {"error": "No weekly clusters found."}

        weekly_insights = self.analyze_weekly_trends_with_web_search(weekly_clusters)
        return weekly_insights

    # -------------------------------------------------------------------------
    # MONTHLY CLUSTERS (NEW)
    # -------------------------------------------------------------------------
    def get_months_clusters(self, date_override=None):
        """
        Fetch clusters for the current month (or override date's month).
        The doc ID is 'monthly_{YYYY}_{MM}' e.g. 'monthly_2023_07'.
        """
        if date_override:
            try:
                dt = datetime.datetime.strptime(date_override, '%Y-%m-%d')
            except ValueError:
                logger.warning(f"Invalid date_override format: {date_override}. Using current date.")
                dt = datetime.datetime.now()
        else:
            dt = datetime.datetime.now()
        
        year = dt.year
        month_num = dt.month
        monthly_doc_id = f"monthly_{year}_{month_num:02d}"
        
        logger.info(f"Fetching monthly clusters document: {monthly_doc_id}")
        clusters_ref = self.db.collection('clustered_trends').document(monthly_doc_id)
        
        try:
            doc = clusters_ref.get()
            if doc.exists:
                data = doc.to_dict()
                clusters = data.get('trends', [])
                logger.info(f"Found {len(clusters)} clusters for {month_num}/{year}")
                return clusters
            else:
                logger.warning(f"No monthly clusters found for {monthly_doc_id}")
                return []
        except Exception as e:
            logger.error(f"Error fetching monthly clusters: {e}")
            logger.error(traceback.format_exc())
            return []

    def analyze_monthly_trends_with_web_search(self, monthly_clusters):
        """
        Use ChatGPT to provide monthly market insights:
         - Up/down overall this month?
         - Why it moved that way? (Fed, inflation, etc.)
         - Notable gainers/losers
        """
        if not self.api_key:
            logger.error("No OpenAI API key available for monthly analysis.")
            return {"error": "No API key"}

        clusters_text = "THIS MONTH'S ARTICLE CLUSTERS:\n"
        sorted_clusters = sorted(monthly_clusters, key=lambda x: x.get('article_count', 0), reverse=True)
        for i, cluster in enumerate(sorted_clusters[:10]):  # top 10
            keywords = ', '.join(cluster.get('keywords', [])[:5])
            article_count = cluster.get('article_count', 0)
            clusters_text += f"Cluster {i+1}: Keywords [{keywords}] - {article_count} articles\n"
            
            article_ids = cluster.get('articles', [])
            sample_titles = self.get_article_titles(article_ids, max_titles=3)
            for title in sample_titles:
                clusters_text += f"    * {title}\n"
            
            if 'topic' in cluster:
                clusters_text += f"    Generated topic: \"{cluster['topic']}\"\n"
            
            clusters_text += "\n"

        prompt = f"""
You are a monthly financial news analyst. Summarize how the markets performed *this month* based on the clusters below and current financial news from the web.

{clusters_text}

Please answer:
1. Did the market go up, down, or stay fairly stable overall this month? (Call it "market_move")
2. Briefly explain why it moved that way (e.g., inflation, Fed, major earnings).
3. Include any interesting monthly highlights (big winners, losers, major stories).
4. Provide your final insights in JSON format, like:
{{
  "month_of": "YYYY-MM",
  "market_move": "up|down|flat",
  "brief_explanation": "...",
  "highlights": ["CompanyX soared 15%", "Gold prices dropped", ...],
  "reasoning": "More detailed monthly explanation or context behind the move."
}}

IMPORTANT: Provide ONLY valid JSON as your final output.
        """

        logger.info("Calling ChatGPT for monthly trend analysis")
        
        try:
            response = openai.ChatCompletion.create(
                model="gpt-4o-search-preview",
                web_search_options={"search_context_size": "medium"},
                messages=[
                    {"role": "system", "content": "You are a monthly market analyst that responds in valid JSON only."},
                    {"role": "user", "content": prompt}
                ]
            )
            
            # Fallback
            response = openai.ChatCompletion.create(
                model="gpt-4o-search-preview",
                messages=[
                    {"role": "system", "content": "You are a monthly market analyst that responds in valid JSON only."},
                    {"role": "user", "content": prompt}
                ]
            )
            
            content = response.choices[0].message.content
            logger.info(f"Received monthly analysis: {content[:100]}...")

            json_start = content.find('{')
            json_end = content.rfind('}') + 1
            if json_start >= 0 and json_end > json_start:
                return json.loads(content[json_start:json_end])
            else:
                logger.error("No valid JSON found in the GPT monthly analysis response.")
                return {"error": "Invalid JSON response"}
        except Exception as e:
            logger.error(f"Error calling ChatGPT for monthly analysis: {e}")
            logger.error(traceback.format_exc())
            return {"error": str(e)}

    def run_monthly_trend_analysis(self, date_override=None):
        """
        Retrieve monthly clusters (or override date's month),
        then get GPT insights. 
        """
        logger.info("Starting monthly trend analysis")
        
        monthly_clusters = self.get_months_clusters(date_override=date_override)
        if not monthly_clusters:
            return {"error": "No monthly clusters found."}

        monthly_insights = self.analyze_monthly_trends_with_web_search(monthly_clusters)
        return monthly_insights

# -------------------------------------------------------------------------
# Database store functions
# -------------------------------------------------------------------------
def store_daily_analysis(db, analysis):
    """
    Store the daily analysis results in Firestore.
    """
    try:
        if 'date' not in analysis:
            analysis['date'] = datetime.datetime.now().strftime('%Y-%m-%d')
        doc_id = f"analysis_{analysis['date']}"
        analysis['timestamp'] = datetime.datetime.now()
        
        db.collection('daily_analysis').document(doc_id).set(analysis)
        logger.info(f"Stored daily analysis for {analysis['date']}")
    except Exception as e:
        logger.error(f"Error storing daily analysis: {e}")
        logger.error(traceback.format_exc())

def store_weekly_analysis(db, analysis):
    """
    Store the weekly analysis results in Firestore.
    """
    try:
        if 'week_of' not in analysis:
            dt = datetime.datetime.now()
            week_num = dt.isocalendar()[1]
            analysis['week_of'] = f"{dt.year}-W{week_num}"
        
        doc_id = f"analysis_{analysis['week_of']}"
        analysis['timestamp'] = datetime.datetime.now()
        
        db.collection('weekly_analysis').document(doc_id).set(analysis)
        logger.info(f"Stored weekly analysis for {analysis['week_of']}")
    except Exception as e:
        logger.error(f"Error storing weekly analysis: {e}")
        logger.error(traceback.format_exc())


def store_monthly_analysis(db, analysis):
    """
    Store the monthly analysis results in Firestore.
    """
    try:
        # If GPT doesn't provide a 'month_of', we'll generate it
        if 'month_of' not in analysis:
            dt = datetime.datetime.now()
            analysis['month_of'] = f"{dt.year}-{dt.month:02d}"
        
        doc_id = f"analysis_{analysis['month_of']}"
        analysis['timestamp'] = datetime.datetime.now()
        
        db.collection('monthly_analysis').document(doc_id).set(analysis)
        logger.info(f"Stored monthly analysis for {analysis['month_of']}")
    except Exception as e:
        logger.error(f"Error storing monthly analysis: {e}")
        logger.error(traceback.format_exc())


# -------------------------------------------------------------------------
# Public entry points
# -------------------------------------------------------------------------
def run_daily_trend_analysis(db, date_override=None):
    """
    Run the daily trend analysis end-to-end.
    """
    api_key = os.getenv('OPENAI_API_KEY')
    manager = DailyTrendManager(db, api_key)
    results = manager.run_daily_trend_analysis(date_override=date_override)
    
    if "error" in results:
        logger.error(f"Daily analysis error: {results['error']}")
        print(f"Error: {results['error']}")
    else:
        print("Daily Trend Analysis Results (JSON):")
        print(json.dumps(results, indent=2))
        store_daily_analysis(db, results)
        logger.info("Daily trend analysis completed successfully")


def run_weekly_trend_analysis(db, date_override=None):
    """
    Run the weekly trend analysis end-to-end.
    """
    api_key = os.getenv('OPENAI_API_KEY')
    manager = DailyTrendManager(db, api_key)
    results = manager.run_weekly_trend_analysis(date_override=date_override)
    
    if "error" in results:
        logger.error(f"Weekly analysis error: {results['error']}")
        print(f"Error: {results['error']}")
    else:
        print("Weekly Trend Analysis Results (JSON):")
        print(json.dumps(results, indent=2))
        store_weekly_analysis(db, results)
        logger.info("Weekly trend analysis completed successfully")


def run_monthly_trend_analysis(db, date_override=None):
    """
    Run the monthly trend analysis end-to-end.
    """
    api_key = os.getenv('OPENAI_API_KEY')
    manager = DailyTrendManager(db, api_key)
    results = manager.run_monthly_trend_analysis(date_override=date_override)
    
    if "error" in results:
        logger.error(f"Monthly analysis error: {results['error']}")
        print(f"Error: {results['error']}")
    else:
        print("Monthly Trend Analysis Results (JSON):")
        print(json.dumps(results, indent=2))
        store_monthly_analysis(db, results)
        logger.info("Monthly trend analysis completed successfully")

# -------------------------------------------------------------------------
# If run from CLI
# -------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse
    from firebase_admin import credentials, firestore
    import firebase_admin
    
    # Setup argument parser
    parser = argparse.ArgumentParser(description='Run market trend analysis')
    parser.add_argument('--daily', action='store_true', help='Run daily analysis')
    parser.add_argument('--weekly', action='store_true', help='Run weekly analysis')
    parser.add_argument('--monthly', action='store_true', help='Run monthly analysis')
    parser.add_argument('--override_date', type=str, default=None, help='Override date YYYY-MM-DD')
    args = parser.parse_args()

    # Initialize Firebase
    try:
        if not firebase_admin._apps:
            firebase_creds_path = os.getenv('FIREBASE_CREDENTIALS_PATH', 'server/market-sentiment-de-llm-firebase-adminsdk-fbsvc-9b39350acd.json')
            cred = credentials.Certificate(firebase_creds_path)
            firebase_admin.initialize_app(cred)
        db = firestore.client()
        
        # If no flags specified, run daily
        if not (args.daily or args.weekly or args.monthly):
            run_daily_trend_analysis(db, date_override=args.override_date)
        else:
            if args.daily:
                cluster_trends.get_daily_clustered_trends()
                run_daily_trend_analysis(db, date_override=args.override_date)
            if args.weekly:
                cluster_trends.get_weekly_clustered_trends()
                run_weekly_trend_analysis(db, date_override=args.override_date)
                topic_analysis_update.run_weekly_topic_management(db)
            if args.monthly:
                cluster_trends.get_monthly_clustered_trends()
                run_monthly_trend_analysis(db, date_override=args.override_date)
                topic_analysis_update.run_monthly_topic_management(db)

    except Exception as e:
        print(f"Error initializing trend analysis: {e}")
        traceback.print_exc()

