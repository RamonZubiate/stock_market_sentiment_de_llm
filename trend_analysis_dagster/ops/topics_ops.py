from dagster import op, Out, In
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
import json
import traceback

logger = logging.getLogger(__name__)

MAX_TOPICS = 20  # Maximum allowed number of topics in the database

@op(
    out={"current_topics": Out()},
    required_resource_keys={"firestore"}
)
def get_current_topics(context):
    """Get all topics from the database with their metrics."""
    topics_ref = context.resources.firestore.collection('topics')
    topics = []
    
    try:
        for doc in topics_ref.stream():
            topic_data = doc.to_dict()
            topic_data['id'] = doc.id  # Add document ID for reference
            topics.append(topic_data)
        
        return topics
    except Exception as e:
        context.log.error(f"Error getting topics: {e}")
        return []

@op(
    out={"weekly_clusters": Out()},
    required_resource_keys={"firestore"}
)
def get_weekly_clusters(context):
    """Get the current week's article clusters from the database."""
    today = datetime.now()
    current_week = today.isocalendar()[1]
    current_year = today.year
    
    weekly_doc_id = f"weekly_{current_year}_W{current_week}"
    clusters_ref = context.resources.firestore.collection('clustered_trends').document(weekly_doc_id)
    
    try:
        doc = clusters_ref.get()
        if doc.exists:
            context.log.info(f"Found weekly clusters document: {weekly_doc_id}")
            clusters_data = doc.to_dict()
            return clusters_data.get('trends', [])
        else:
            context.log.warning(f"No weekly clusters found for {weekly_doc_id}")
            
            # Try the previous week as fallback
            prev_date = today - timedelta(days=7)
            prev_week = prev_date.isocalendar()[1]
            prev_year = prev_date.year
            prev_weekly_doc_id = f"weekly_{prev_year}_W{prev_week}"
            prev_clusters_ref = context.resources.firestore.collection('clustered_trends').document(prev_weekly_doc_id)
            
            prev_doc = prev_clusters_ref.get()
            if prev_doc.exists:
                context.log.info(f"Using previous week's clusters as fallback: {prev_weekly_doc_id}")
                clusters_data = prev_doc.to_dict()
                return clusters_data.get('trends', [])
            
            # If no weekly documents exist, try to collect the past 7 days of daily clusters
            context.log.info("No weekly clusters found, collecting daily clusters from the past 7 days")
            return collect_past_week_daily_clusters(context)
    except Exception as e:
        context.log.error(f"Error getting weekly clusters: {e}")
        raise

@op(
    out={"daily_clusters": Out()},
    required_resource_keys={"firestore"}
)
def collect_past_week_daily_clusters(context):
    """Collect clusters from the past 7 days of daily cluster documents."""
    combined_clusters = []
    today = datetime.now()
    
    for i in range(7):
        date = today - timedelta(days=i)
        date_str = date.strftime('%Y-%m-%d')
        daily_doc_id = f"daily_{date_str}"
        
        try:
            doc = context.resources.firestore.collection('clustered_trends').document(daily_doc_id).get()
            if doc.exists:
                clusters_data = doc.to_dict()
                daily_clusters = clusters_data.get('trends', [])
                
                # Add date info to each cluster
                for cluster in daily_clusters:
                    cluster['source_date'] = date_str
                
                combined_clusters.extend(daily_clusters)
                context.log.info(f"Added {len(daily_clusters)} clusters from {date_str}")
        except Exception as e:
            context.log.warning(f"Error getting clusters for {date_str}: {e}")
    
    context.log.info(f"Collected {len(combined_clusters)} clusters from the past 7 days")
    return combined_clusters

@op(
    ins={"article_ids": In()},
    out={"article_titles": Out()},
    required_resource_keys={"firestore"}
)
def get_article_titles(context, article_ids: List[str], max_titles: int = 10) -> List[str]:
    """Get article titles from a list of article IDs."""
    titles = []
    articles_ref = context.resources.firestore.collection('articles')
    
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
        context.log.error(f"Error getting article titles: {e}")
        return []

@op(
    ins={"current_topics": In(), "clusters": In()},
    out={"topic_analysis": Out()},
    required_resource_keys={"openai"}
)
def analyze_topics_with_web_search(context, current_topics: List[Dict[str, Any]], clusters: List[Dict[str, Any]], period: str = "weekly"):
    """Use ChatGPT with web search to analyze topics and recommend changes."""
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
        sample_titles = get_article_titles(context, article_ids, max_titles=5)
        
        for title in sample_titles:
            clusters_text += f"* \"{title}\"\n"
        
        if 'topic' in cluster:
            clusters_text += f"Generated topic: \"{cluster['topic']}\"\n"

    # Build the prompt
    prompt = f"""
You are a financial market analyst tasked with managing a list of topics to track in the market. Your job is to analyze the current topics and recent market clusters to determine which topics should be added, removed, or modified.

Current Topics:
{topics_text}

Recent Market Clusters:
{clusters_text}

Please analyze this information and provide recommendations in the following JSON format:
{{
  "add_topics": [
    {{
      "name": "Topic name",
      "keyword": "Search keyword",
      "category_id": "Category ID",
      "reason": "Why this topic should be added"
    }}
  ],
  "remove_topics": [
    {{
      "id": "Topic ID to remove",
      "reason": "Why this topic should be removed"
    }}
  ],
  "modify_topics": [
    {{
      "id": "Topic ID to modify",
      "changes": {{
        "name": "New name (if changing)",
        "keyword": "New keyword (if changing)",
        "category_id": "New category (if changing)",
        "active": true/false (if changing status)
      }},
      "reason": "Why these changes are needed"
    }}
  ],
  "analysis": "Brief summary of your analysis and recommendations"
}}

IMPORTANT:
1. Only recommend removing topics if they are clearly no longer relevant
2. Only add new topics if they represent significant, ongoing market themes
3. Keep the total number of active topics under {MAX_TOPICS}
4. Provide clear reasoning for each change
5. Return ONLY valid JSON
"""

    context.log.info(f"Calling ChatGPT for {period} topic analysis with web search")
    
    try:
        # Attempt with web search
        response = context.resources.openai.ChatCompletion.create(
            model="gpt-4o-search-preview",
            web_search_options={"search_context_size": "low"},
            messages=[
                {"role": "system", "content": "You are a market analyst that responds in valid JSON only."},
                {"role": "user", "content": prompt}
            ]
        )
        
        # Fallback if needed
        response = context.resources.openai.ChatCompletion.create(
            model="gpt-4o-search-preview",
            messages=[
                {"role": "system", "content": "You are a market analyst that responds in valid JSON only."},
                {"role": "user", "content": prompt}
            ]
        )
        
        content = response.choices[0].message.content
        context.log.info(f"Received topic analysis: {content[:100]}...")

        json_start = content.find('{')
        json_end = content.rfind('}') + 1
        if json_start >= 0 and json_end > json_start:
            json_content = content[json_start:json_end]
            return json.loads(json_content)
        else:
            context.log.error("No valid JSON found in the GPT topic analysis response.")
            return {"error": "Invalid JSON response"}
    except Exception as e:
        context.log.error(f"Error calling ChatGPT for topic analysis: {e}")
        raise

@op(
    ins={"current_topics": In(), "clusters": In()},
    out={"topic_analysis": Out()},
    required_resource_keys={"openai"}
)
def analyze_topics_without_web_search(context, current_topics: List[Dict[str, Any]], clusters: List[Dict[str, Any]], period: str = "weekly"):
    """
    Use ChatGPT (without web search) to analyze topics and recommend changes.
    This is a fallback for when web search is not available.
    """
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
        sample_titles = get_article_titles(context, article_ids, max_titles=5)
        
        for title in sample_titles:
            clusters_text += f"* \"{title}\"\n"
        
        if 'topic' in cluster:
            clusters_text += f"Generated topic: \"{cluster['topic']}\"\n"

    # Build the prompt
    prompt = f"""
You are a financial market analyst tasked with managing a list of topics to track in the market. Your job is to analyze the current topics and recent market clusters to determine which topics should be added, removed, or modified.

Current Topics:
{topics_text}

Recent Market Clusters:
{clusters_text}

Please analyze this information and provide recommendations in the following JSON format:
{{
  "add_topics": [
    {{
      "name": "Topic name",
      "keyword": "Search keyword",
      "category_id": "Category ID",
      "reason": "Why this topic should be added"
    }}
  ],
  "remove_topics": [
    {{
      "id": "Topic ID to remove",
      "reason": "Why this topic should be removed"
    }}
  ],
  "modify_topics": [
    {{
      "id": "Topic ID to modify",
      "changes": {{
        "name": "New name (if changing)",
        "keyword": "New keyword (if changing)",
        "category_id": "New category (if changing)",
        "active": true/false (if changing status)
      }},
      "reason": "Why these changes are needed"
    }}
  ],
  "analysis": "Brief summary of your analysis and recommendations"
}}

IMPORTANT:
1. Only recommend removing topics if they are clearly no longer relevant
2. Only add new topics if they represent significant, ongoing market themes
3. Keep the total number of active topics under {MAX_TOPICS}
4. Provide clear reasoning for each change
5. Return ONLY valid JSON
"""

    logger.info(f"Calling ChatGPT for {period} topic analysis without web search")
    
    try:
        response = context.resources.openai.ChatCompletion.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "You are a market analyst that responds in valid JSON only."},
                {"role": "user", "content": prompt}
            ]
        )
        
        content = response.choices[0].message.content
        logger.info(f"Received topic analysis: {content[:100]}...")

        json_start = content.find('{')
        json_end = content.rfind('}') + 1
        if json_start >= 0 and json_end > json_start:
            json_content = content[json_start:json_end]
            return json.loads(json_content)
        else:
            logger.error("No valid JSON found in the GPT topic analysis response.")
            return {"error": "Invalid JSON response"}
    except Exception as e:
        logger.error(f"Error calling ChatGPT for topic analysis: {e}")
        logger.error(traceback.format_exc())
        return {"error": str(e)}

@op(
    ins={"topic_analysis": In()},
    out={"topic_changes": Out()},
    required_resource_keys={"firestore"}
)
def apply_topic_changes(context, topic_analysis: Dict[str, Any]):
    """Apply the recommended topic changes to the database."""
    results = {
        "added": [],
        "removed": [],
        "modified": [],
        "errors": []
    }
    
    topics_ref = context.resources.firestore.collection('topics')
    
    try:
        # Add new topics
        for topic in topic_analysis.get('add_topics', []):
            try:
                # Check if we're at the topic limit
                active_topics = [t for t in get_current_topics(context) if t.get('active', False)]
                if len(active_topics) >= MAX_TOPICS:
                    context.log.warning(f"Cannot add topic '{topic['name']}': Maximum topic limit reached")
                    results["errors"].append(f"Cannot add topic '{topic['name']}': Maximum topic limit reached")
                    continue
                
                # Create new topic document
                topic_data = {
                    "name": topic['name'],
                    "keyword": topic['keyword'],
                    "category_id": topic['category_id'],
                    "active": True,
                    "created_at": datetime.now().isoformat(),
                    "last_updated": datetime.now().isoformat()
                }
                
                doc_ref = topics_ref.document()
                doc_ref.set(topic_data)
                results["added"].append({
                    "id": doc_ref.id,
                    "name": topic['name'],
                    "reason": topic['reason']
                })
                context.log.info(f"Added new topic: {topic['name']}")
            except Exception as e:
                context.log.error(f"Error adding topic {topic['name']}: {e}")
                results["errors"].append(f"Error adding topic {topic['name']}: {e}")
        
        # Remove topics
        for topic in topic_analysis.get('remove_topics', []):
            try:
                doc = topics_ref.document(topic['id']).get()
                if doc.exists:
                    doc.reference.delete()
                    results["removed"].append({
                        "id": topic['id'],
                        "reason": topic['reason']
                    })
                    context.log.info(f"Removed topic: {topic['id']}")
                else:
                    context.log.warning(f"Topic to remove not found: {topic['id']}")
                    results["errors"].append(f"Topic to remove not found: {topic['id']}")
            except Exception as e:
                context.log.error(f"Error removing topic {topic['id']}: {e}")
                results["errors"].append(f"Error removing topic {topic['id']}: {e}")
        
        # Modify topics
        for topic in topic_analysis.get('modify_topics', []):
            try:
                doc = topics_ref.document(topic['id']).get()
                if doc.exists:
                    changes = topic['changes']
                    update_data = {}
                    
                    if 'name' in changes:
                        update_data['name'] = changes['name']
                    if 'keyword' in changes:
                        update_data['keyword'] = changes['keyword']
                    if 'category_id' in changes:
                        update_data['category_id'] = changes['category_id']
                    if 'active' in changes:
                        update_data['active'] = changes['active']
                    
                    update_data['last_updated'] = datetime.now().isoformat()
                    
                    doc.reference.update(update_data)
                    results["modified"].append({
                        "id": topic['id'],
                        "changes": changes,
                        "reason": topic['reason']
                    })
                    context.log.info(f"Modified topic: {topic['id']}")
                else:
                    context.log.warning(f"Topic to modify not found: {topic['id']}")
                    results["errors"].append(f"Topic to modify not found: {topic['id']}")
            except Exception as e:
                context.log.error(f"Error modifying topic {topic['id']}: {e}")
                results["errors"].append(f"Error modifying topic {topic['id']}: {e}")
        
        return results
    except Exception as e:
        context.log.error(f"Error applying topic changes: {e}")
        raise

@op(
    ins={"period": In(), "topic_analysis": In()},
    out={"analysis_stored": Out()},
    required_resource_keys={"firestore"}
)
def store_topic_analysis(context, period: str, topic_analysis: Dict[str, Any]):
    """Store the topic analysis results in Firestore."""
    try:
        analysis_data = {
            "period": period,
            "analysis": topic_analysis,
            "timestamp": datetime.now().isoformat()
        }
        
        doc_id = f"{period}_topic_analysis_{datetime.now().strftime('%Y-%m-%d')}"
        context.resources.firestore.collection('topic_analysis').document(doc_id).set(analysis_data)
        context.log.info(f"Stored {period} topic analysis")
        return True
    except Exception as e:
        context.log.error(f"Error storing topic analysis: {e}")
        raise

@op(
    ins={"topic_analysis": In(), "topic_changes": In()},
    out={"log_stored": Out()},
    required_resource_keys={"firestore"}
)
def log_topic_management(context, topic_analysis: Dict[str, Any], topic_changes: Dict[str, Any], period: str = "weekly"):
    """Log the topic management actions and results."""
    try:
        log_data = {
            "period": period,
            "timestamp": datetime.now().isoformat(),
            "analysis": topic_analysis,
            "changes": topic_changes
        }
        
        doc_id = f"{period}_topic_management_{datetime.now().strftime('%Y-%m-%d')}"
        context.resources.firestore.collection('topic_management_logs').document(doc_id).set(log_data)
        context.log.info(f"Logged {period} topic management actions")
        return True
    except Exception as e:
        context.log.error(f"Error logging topic management: {e}")
        raise

@op(
    out={"weekly_topic_management": Out()},
    required_resource_keys={"firestore", "openai"}
)
def run_weekly_topic_management(context):
    """Run the weekly topic management pipeline."""
    try:
        # Get current topics and weekly clusters
        current_topics = get_current_topics(context)
        clusters = get_weekly_clusters(context)
        
        if not clusters:
            logger.warning("No weekly clusters found for topic management")
            return None
            
        # Try analysis with web search first
        try:
            analysis = analyze_topics_with_web_search(context, current_topics, clusters, period="weekly")
        except Exception as e:
            logger.warning(f"Web search analysis failed, falling back to regular analysis: {e}")
            analysis = analyze_topics_without_web_search(context, current_topics, clusters, period="weekly")
        
        if not analysis or "error" in analysis:
            logger.error("Failed to generate topic analysis")
            return None
        
        # Apply the recommended changes
        changes = apply_topic_changes(context, analysis)
        
        # Store the analysis and log the changes
        store_topic_analysis(context, "weekly", analysis)
        log_topic_management(context, analysis, changes, period="weekly")
        
        return {
            "date": datetime.now().isoformat(),
            "analysis": analysis,
            "changes": changes
        }
    except Exception as e:
        logger.error(f"Error in weekly topic management: {e}")
        logger.error(traceback.format_exc())
        return None
