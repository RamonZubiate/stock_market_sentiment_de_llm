from dagster import op, Out, In
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import json
import traceback

@op(
    out={"daily_clusters": Out()},
    required_resource_keys={"firestore"}
)
def get_todays_clusters(context, date_override=None):
    """
    Fetch clusters for today's date (or an override date) from Firestore.
    """
    if date_override:
        target_date = date_override
    else:
        target_date = datetime.now().strftime('%Y-%m-%d')
    
    daily_doc_id = f"daily_{target_date}"
    context.log.info(f"Fetching daily clusters document: {daily_doc_id}")

    # Get the Firestore DB client first
    db = context.resources.firestore.get_db()
    clusters_ref = db.collection('clustered_trends').document(daily_doc_id)
    
    try:
        doc = clusters_ref.get()
        if doc.exists:
            data = doc.to_dict()
            clusters = data.get('trends', [])
            context.log.info(f"Found {len(clusters)} clusters for {target_date}")
            # Add detailed logging about clusters
            for i, cluster in enumerate(clusters):
                context.log.info(f"Cluster {i+1}: {len(cluster.get('articles', []))} articles, "
                          f"Keywords: {', '.join(cluster.get('keywords', [])[:5])}")
            return clusters
        else:
            context.log.warning(f"No daily clusters found for {target_date}")
            return []
    except Exception as e:
        context.log.error(f"Error fetching today's clusters: {e}")
        context.error(traceback.format_exc())
        return []
# Fixed get_article_titles function
def get_article_titles(db, article_ids: List[str], max_titles: int = 5) -> List[str]:
    """Retrieve a few article titles for context in the GPT prompt."""
    titles = []
    
    try:
        articles_ref = db.collection('articles')
        
        for article_id in article_ids[:max_titles]:
            doc = articles_ref.document(article_id).get()
            if doc.exists:
                article_data = doc.to_dict()
                title = article_data.get('title', '')
                if title:
                    titles.append(title)
        return titles
    except Exception as e:
        logging.error(f"Error fetching article titles: {e}")
        return []

@op(
    ins={"weekly_clusters": In()},
    out={"weekly_analysis": Out()},
    required_resource_keys={"openai", "firestore"}  # Added firestore to required resources
)
def analyze_weekly_trends_with_web_search(context, weekly_clusters):
    """
    Use ChatGPT to provide weekly market insights.
    """
    
    # Get the Firestore DB client
    db = context.resources.firestore.get_db()

    clusters_text = "THIS WEEK'S ARTICLE CLUSTERS:\n"
    sorted_clusters = sorted(weekly_clusters, key=lambda x: x.get('article_count', 0), reverse=True)
    for i, cluster in enumerate(sorted_clusters[:10]):  # top 10
        keywords = ', '.join(cluster.get('keywords', [])[:5])
        article_count = cluster.get('article_count', 0)
        clusters_text += f"Cluster {i+1}: Keywords [{keywords}] - {article_count} articles\n"
        
        article_ids = cluster.get('articles', [])
        sample_titles = get_article_titles(db, article_ids, max_titles=3)  # Fix: pass db instead of context
        for title in sample_titles:
            clusters_text += f"    * {title}\n"
        
        if 'topic' in cluster:
            clusters_text += f"    Generated topic: \"{cluster['topic']}\"\n"
        
        clusters_text += "\n"

    prompt = f"""
You are a weekly financial news analyst. Your job is to summarize the key market themes and trends from this week based on the article clusters below and current financial news from the web.

{clusters_text}

Please analyze and provide your insights in JSON format:
{{
  "week_ending": "YYYY-MM-DD",
  "market_summary": "Brief overview of how markets performed this week",
  "key_themes": [
    {{
      "theme": "Theme name",
      "description": "Brief description",
      "impact": "Positive/Negative/Neutral"
    }}
  ],
  "notable_events": ["Event 1", "Event 2", ...],
  "outlook": "Brief forward-looking analysis"
}}

IMPORTANT: Provide ONLY valid JSON as your final output.
    """

    context.log.info("Calling ChatGPT for weekly trend analysis with web search")
    
    try:
        # Attempt with web search
        response = context.resources.openai.generate_completion(
            model="gpt-4o-search-preview",
            web_search_options={"search_context_size": "low"},
            messages=[
                {"role": "system", "content": "You are a weekly market analyst that responds in valid JSON only."},
                {"role": "user", "content": prompt}
            ]
        )
        
        content = response.choices[0].message.content
        context.log.info(f"Received weekly analysis: {content}...")

        json_start = content.find('{')
        json_end = content.rfind('}') + 1
        if json_start >= 0 and json_end > json_start:
            json_content = content[json_start:json_end]
            return json.loads(json_content)
        else:
            context.log.error("No valid JSON found in the GPT weekly analysis response.")
            return {"error": "Invalid JSON response"}
    except Exception as e:
        context.log.error(f"Error calling ChatGPT for weekly analysis: {e}")
        context.log.error(traceback.format_exc())
        return {"error": str(e)}

@op(
    ins={"monthly_clusters": In()},
    out={"monthly_analysis": Out()},
    required_resource_keys={"openai", "firestore"}  # Added firestore to required resources
)
def analyze_monthly_trends_with_web_search(context, monthly_clusters):
    """
    Use ChatGPT to provide monthly market insights.
    """
    
    # Get the Firestore DB client
    db = context.resources.firestore.get_db()

    clusters_text = "THIS MONTH'S ARTICLE CLUSTERS:\n"
    sorted_clusters = sorted(monthly_clusters, key=lambda x: x.get('article_count', 0), reverse=True)
    for i, cluster in enumerate(sorted_clusters[:15]):  # top 15 for monthly
        keywords = ', '.join(cluster.get('keywords', [])[:5])
        article_count = cluster.get('article_count', 0)
        clusters_text += f"Cluster {i+1}: Keywords [{keywords}] - {article_count} articles\n"
        
        article_ids = cluster.get('articles', [])
        sample_titles = get_article_titles(db, article_ids, max_titles=3)  # Fix: pass db instead of context
        for title in sample_titles:
            clusters_text += f"    * {title}\n"
        
        if 'topic' in cluster:
            clusters_text += f"    Generated topic: \"{cluster['topic']}\"\n"
        
        clusters_text += "\n"

    prompt = f"""
You are a monthly financial news analyst. Your job is to summarize the key market themes, trends, and shifts from this month based on the article clusters below and current financial news from the web.

{clusters_text}

Please analyze and provide your insights in JSON format:
{{
  "month": "YYYY-MM",
  "market_summary": "Overview of market performance this month",
  "major_themes": [
    {{
      "theme": "Theme name",
      "trend": "Emerging/Continuing/Fading",
      "description": "Analysis of the theme",
      "impact": "Long-term implications"
    }}
  ],
  "key_developments": ["Development 1", "Development 2", ...],
  "sector_analysis": [
    {{
      "sector": "Sector name",
      "performance": "Brief performance summary",
      "outlook": "Forward-looking view"
    }}
  ],
  "monthly_outlook": "Comprehensive forward-looking analysis"
}}

IMPORTANT: Provide ONLY valid JSON as your final output.
    """

    context.log.info("Calling ChatGPT for monthly trend analysis with web search")
    
    try:
        response = context.resources.openai.generate_completion(
            model="gpt-4o-search-preview",
            web_search_options={"search_context_size": "low"},
            messages=[
                {"role": "system", "content": "You are a monthly market analyst that responds in valid JSON only."},
                {"role": "user", "content": prompt}
            ]
        )
        
        content = response.choices[0].message.content
        context.log.info(f"Received monthly analysis: {content}...")

        json_start = content.find('{')
        json_end = content.rfind('}') + 1
        if json_start >= 0 and json_end > json_start:
            json_content = content[json_start:json_end]
            return json.loads(json_content)
        else:
            context.log.error("No valid JSON found in the GPT monthly analysis response.")
            return {"error": "Invalid JSON response"}
    except Exception as e:
        context.log.error(f"Error calling ChatGPT for monthly analysis: {e}")
        context.log.error(traceback.format_exc())
        return {"error": str(e)}

@op(
    ins={"daily_clusters": In(), "daily_analysis": In()},
    out={"analysis_stored": Out()},
    required_resource_keys={"firestore"}
)
def store_daily_analysis(context, daily_clusters, daily_analysis):
    """Store the daily trend analysis results in Firestore."""
    try:
        if not daily_analysis or "error" in daily_analysis:
            context.log.error("Invalid daily analysis to store")
            return False

        today = datetime.now()
        doc_id = f"daily_analysis_{today.strftime('%Y-%m-%d')}"
        
        analysis_data = {
            "date": today.isoformat(),
            "clusters": daily_clusters,
            "analysis": daily_analysis,
            "timestamp": today.isoformat()
        }
        
        # Get the Firestore DB client first
        db = context.resources.firestore.get_db()
        db.collection('daily_analysis').document(doc_id).set(analysis_data)
        context.log.info(f"Stored daily analysis: {doc_id}")
        return True
    except Exception as e:
        context.log.error(f"Error storing daily analysis: {e}")
        context.log.error(traceback.format_exc())
        return False
@op(
    ins={"daily_clusters": In()},
    out={"daily_analysis": Out()},
    required_resource_keys={"openai", "firestore"} 
)
def analyze_daily_trends_with_web_search(context, daily_clusters):
    """
    Use ChatGPT (with optional web search) to provide daily market insights.
    """
    if not daily_clusters:
        return {"error": "No daily clusters provided"}
    
    db = context.resources.firestore.get_db()

    # Format daily clusters for the prompt
    clusters_text = "TODAY'S ARTICLE CLUSTERS:\n"
    sorted_clusters = sorted(daily_clusters, key=lambda x: x.get('article_count', 0), reverse=True)
    for i, cluster in enumerate(sorted_clusters[:10]):  # limit to top 10 for brevity
        keywords = ', '.join(cluster.get('keywords', [])[:5])
        article_count = cluster.get('article_count', 0)
        clusters_text += f"Cluster {i+1}: Keywords [{keywords}] - {article_count} articles\n"
        
        article_ids = cluster.get('articles', [])
        sample_titles = get_article_titles(db, article_ids, max_titles=3)
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

    context.log.info("Calling ChatGPT for daily trend analysis with web search")
    
    try:
        # Attempt with web search
        response = context.resources.openai.generate_completion(
            model="gpt-4o-search-preview",
            web_search_options={"search_context_size": "low"},
            messages=[
                {"role": "system", "content": "You are a daily market analyst that responds in valid JSON only."},
                {"role": "user", "content": prompt}
            ]
        )
        
        content = response.choices[0].message.content
        context.log.info(f"Received daily analysis: {content}...")

        json_start = content.find('{')
        json_end = content.rfind('}') + 1
        if json_start >= 0 and json_end > json_start:
            json_content = content[json_start:json_end]
            return json.loads(json_content)
        else:
            context.log.error("No valid JSON found in the GPT daily analysis response.")
            return {"error": "Invalid JSON response"}
    except Exception as e:
        context.log.error(f"Error calling ChatGPT for daily analysis: {e}")
        context.log.error(traceback.format_exc())
        return {"error": str(e)}
@op(
    out={"weekly_clusters": Out()},
    required_resource_keys={"firestore"}
)
def get_weeks_clusters(context, date_override=None):
    """
    Fetch clusters for this week (or an override date).
    The doc ID is 'weekly_{YEAR}_W{WEEK}'.
    """
    if date_override:
        try:
            dt = datetime.strptime(date_override, '%Y-%m-%d')
        except ValueError:
            context.log.warning(f"Invalid date_override format: {date_override}. Using current date.")
            dt = datetime.now()
    else:
        dt = datetime.now()
    
    year = dt.year
    week_number = dt.isocalendar()[1]
    weekly_doc_id = f"weekly_{year}_W{week_number}"
    
    context.log.info(f"Fetching weekly clusters document: {weekly_doc_id}")
    
    # Get the Firestore DB client first
    db = context.resources.firestore.get_db()
    clusters_ref = db.collection('clustered_trends').document(weekly_doc_id)
    
    try:
        doc = clusters_ref.get()
        if doc.exists:
            data = doc.to_dict()
            clusters = data.get('trends', [])
            context.log.info(f"Found {len(clusters)} clusters for week {week_number} of {year}")
            return clusters
        else:
            context.log.warning(f"No weekly clusters found for {weekly_doc_id}")
            return []
    except Exception as e:
        context.log.error(f"Error fetching weekly clusters: {e}")
        context.log.error(traceback.format_exc())
        return []

@op(
    ins={"weekly_clusters": In(), "weekly_analysis": In()},
    out={"analysis_stored": Out()},
    required_resource_keys={"firestore"}
)
def store_weekly_analysis(context, weekly_clusters, weekly_analysis):
    """Store the weekly trend analysis results in Firestore."""
    try:
        if not weekly_analysis or "error" in weekly_analysis:
            context.log.error("Invalid weekly analysis to store")
            return False

        today = datetime.now()
        week_number = today.isocalendar()[1]
        doc_id = f"weekly_analysis_{today.year}_W{week_number}"
        
        analysis_data = {
            "date": today.isoformat(),
            "week": week_number,
            "year": today.year,
            "clusters": weekly_clusters,
            "analysis": weekly_analysis,
            "timestamp": today.isoformat()
        }
        
        # Get the Firestore DB client first
        db = context.resources.firestore.get_db()
        db.collection('trend_analysis').document(doc_id).set(analysis_data)
        context.log.info(f"Stored weekly analysis: {doc_id}")
        return True
    except Exception as e:
        context.log.error(f"Error storing weekly analysis: {e}")
        context.log.error(traceback.format_exc())
        return False

@op(
    out={"monthly_clusters": Out()},
    required_resource_keys={"firestore"}
)
def get_monthly_clusters(context, date_override=None):
    """
    Get the current month's article clusters from the database.
    """
    if date_override:
        try:
            dt = datetime.strptime(date_override, '%Y-%m-%d')
        except ValueError:
            context.log.warning(f"Invalid date_override format: {date_override}. Using current date.")
            dt = datetime.now()
    else:
        dt = datetime.now()
    
    year = dt.year
    month = dt.month
    monthly_doc_id = f"monthly_{year}_{month:02d}"
    
    context.log.info(f"Fetching monthly clusters document: {monthly_doc_id}")
    
    # Get the Firestore DB client first
    db = context.resources.firestore.get_db()
    clusters_ref = db.collection('clustered_trends').document(monthly_doc_id)
    
    try:
        doc = clusters_ref.get()
        if doc.exists:
            data = doc.to_dict()
            clusters = data.get('trends', [])
            context.log.info(f"Found {len(clusters)} clusters for {month}/{year}")
            return clusters
        else:
            context.log.warning(f"No monthly clusters found for {monthly_doc_id}")
            return []
    except Exception as e:
        context.log.error(f"Error fetching monthly clusters: {e}")
        context.log.error(traceback.format_exc())
        return []

@op(
    ins={"monthly_clusters": In(), "monthly_analysis": In()},
    out={"analysis_stored": Out()},
    required_resource_keys={"firestore"}
)
def store_monthly_analysis(context, monthly_clusters, monthly_analysis):
    """Store the monthly trend analysis results in Firestore."""
    try:
        if not monthly_analysis or "error" in monthly_analysis:
            context.log.error("Invalid monthly analysis to store")
            return False

        today = datetime.now()
        doc_id = f"monthly_analysis_{today.year}_{today.month:02d}"
        
        analysis_data = {
            "date": today.isoformat(),
            "month": today.month,
            "year": today.year,
            "clusters": monthly_clusters,
            "analysis": monthly_analysis,
            "timestamp": today.isoformat()
        }
        
        # Get the Firestore DB client first
        db = context.resources.firestore.get_db()
        db.collection('trend_analysis').document(doc_id).set(analysis_data)
        context.log.info(f"Stored monthly analysis: {doc_id}")
        return True
    except Exception as e:
        context.log.error(f"Error storing monthly analysis: {e}")
        context.log.error(traceback.format_exc())
        return False
