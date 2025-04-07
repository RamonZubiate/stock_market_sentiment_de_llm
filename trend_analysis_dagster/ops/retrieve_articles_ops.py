from dagster import op, Out, In, Nothing
import requests
import hashlib
import datetime
import time
from typing import List, Dict, Any
import logging
from datetime import timedelta

logger = logging.getLogger(__name__)

def generate_content_hash(content: str) -> str:
    """Generate a hash of the article content to avoid duplicates"""
    return hashlib.md5(content.encode('utf-8')).hexdigest()

@op(
    out={"articles": Out()},
    config_schema={
        "category": str,
        "query": str,
        "domains": str,
        "count": int
    },
    required_resource_keys={"news_api_key"}
)
def fetch_articles(context) -> List[Dict[str, Any]]:
    """Op to fetch articles from NewsAPI based on category and query."""
    try:
        category = context.op_config["category"]
        query = context.op_config["query"]
        domains = context.op_config.get("domains", "")
        count = context.op_config.get("count", 25)
        
        url = "https://newsapi.org/v2/everything"
        today = datetime.datetime.now().strftime('%Y-%m-%d')
        week_ago = (datetime.datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        
        params = {
            'q': query,
            'apiKey': context.resources.news_api_key,
            'language': 'en',
            'sortBy': 'publishedAt',
            'pageSize': count,
            'from': week_ago,
            'to': today
        }
        
        if domains:
            params['domains'] = domains
            
        response = requests.get(url, params=params)
        response.raise_for_status()
        
        articles = response.json().get('articles', [])
        context.log.info(f"Fetched {len(articles)} articles for category {category}")
        
        # Add category and hash to each article
        for article in articles:
            article['category'] = category
            article['content_hash'] = generate_content_hash(article.get('content', '') + article.get('title', ''))
            
        return articles
    except Exception as e:
        context.log.error(f"Error fetching articles: {str(e)}")
        raise

@op(
    ins={"articles": In()},
    out={"stored_articles": Out()},
    required_resource_keys={"firestore"}
)
def store_articles(context, articles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Op to store articles in Firestore, avoiding duplicates."""
    try:
        db = context.resources.firestore
        stored_articles = []
        
        for article in articles:
            # Check if article already exists
            content_hash = article.get('content_hash')
            if not content_hash:
                continue
                
            # Query for existing article
            existing = db.collection('articles').where('content_hash', '==', content_hash).limit(1).get()
            
            if not existing:
                # Add metadata
                article['stored_at'] = datetime.datetime.now()
                article['processed'] = False
                
                # Store in Firestore
                doc_ref = db.collection('articles').document()
                doc_ref.set(article)
                stored_articles.append(article)
                context.log.info(f"Stored new article: {article.get('title')}")
            else:
                context.log.info(f"Skipped duplicate article: {article.get('title')}")
                
        return stored_articles
    except Exception as e:
        context.log.error(f"Error storing articles: {str(e)}")
        raise

@op(
    out={"categories": Out()},
    required_resource_keys={"firestore"}
)
def fetch_categories(context) -> List[Dict[str, Any]]:
    """Op to fetch categories from Firestore."""
    try:
        db = context.resources.firestore
        categories = []
        
        for doc in db.collection('categories').stream():
            category = doc.to_dict()
            category['id'] = doc.id
            categories.append(category)
            
        context.log.info(f"Fetched {len(categories)} categories")
        return categories
    except Exception as e:
        context.log.error(f"Error fetching categories: {str(e)}")
        raise

@op(
    ins={"category": In()},
    out={"query": Out()}
)
def build_category_query(context, category: Dict[str, Any]) -> str:
    """Op to build search query for a category."""
    try:
        category_id = category.get('id')
        keywords = category.get('keywords', [])
        exclude_terms = category.get('exclude_terms', [])
        
        # Build query string
        query_parts = []
        
        # Add required terms
        for term in keywords:
            if ' ' in term:
                query_parts.append(f'"{term}"')
            else:
                query_parts.append(term)
                
        # Add excluded terms
        for term in exclude_terms:
            if ' ' in term:
                query_parts.append(f'-"{term}"')
            else:
                query_parts.append(f'-{term}')
                
        query = ' AND '.join(query_parts)
        context.log.info(f"Built query for category {category_id}: {query}")
        
        return query
    except Exception as e:
        context.log.error(f"Error building category query: {str(e)}")
        raise

@op(
    out={"collected_articles": Out()},
    required_resource_keys={"firestore", "news_api_key"}
)
def collect_daily_news(context) -> List[Dict[str, Any]]:
    """Op to collect news articles for all categories."""
    try:
        # Fetch categories
        categories = fetch_categories(context)
        
        all_articles = []
        for category in categories:
            # Build query
            query = build_category_query(category)
            
            # Fetch articles
            articles = fetch_articles(context, category=category['id'], query=query)
            
            # Store articles
            stored_articles = store_articles(context, articles)
            all_articles.extend(stored_articles)
            
        context.log.info(f"Collected {len(all_articles)} new articles")
        return all_articles
    except Exception as e:
        context.log.error(f"Error collecting daily news: {str(e)}")
        raise
