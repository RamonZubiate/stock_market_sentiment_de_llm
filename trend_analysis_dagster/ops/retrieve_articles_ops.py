# trend_analysis_dagster/ops/retrieve_articles_ops.py
from dagster import op, Out, In, Nothing, graph
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
        db = context.resources.firestore.get_db()
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
    ins={"categories": In()},
    out={"all_queries": Out()},
    description="Build queries for all categories"
)
def build_all_queries(context, categories):
    """Build search queries for all categories."""
    try:
        category_queries = []
        
        for category in categories:
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
            
            category_queries.append({
                "category_id": category_id,
                "query": query,
                "domains": category.get("domains", ""),
                "count": category.get("article_count", 25)
            })
            
            context.log.info(f"Built query for category {category_id}: {query}")
        
        return category_queries
    except Exception as e:
        context.log.error(f"Error building category queries: {str(e)}")
        raise

@op(
    ins={"category_queries": In()},
    out={"all_articles": Out()},
    required_resource_keys={"news_api_key", "firestore"}
)
def process_all_categories(context, category_queries):
    """Process all categories and return all stored articles."""
    try:
        all_stored_articles = []
        
        for category_info in category_queries:
            category_id = category_info["category_id"]
            query = category_info["query"]
            domains = category_info["domains"]
            count = category_info["count"]
            
            # Fetch articles for this category
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
                
            context.log.info(f"Fetching articles for category {category_id} with query: {query}")
            
            response = requests.get(url, params=params)
            response.raise_for_status()
            
            articles = response.json().get('articles', [])
            context.log.info(f"Fetched {len(articles)} articles for category {category_id}")
            
            # Add category and hash to each article
            for article in articles:
                article['category'] = category_id
                article['content_hash'] = generate_content_hash(article.get('content', '') + article.get('title', ''))
            
            # Store articles - Get the Firestore client using get_db()
            db = context.resources.firestore.get_db()
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
            
            all_stored_articles.extend(stored_articles)
        
        context.log.info(f"Processed all categories, stored {len(all_stored_articles)} articles in total")
        return all_stored_articles
        
    except Exception as e:
        context.log.error(f"Error processing all categories: {str(e)}")
        raise

# Define a simpler graph without dynamic mapping
@graph(name="collect_daily_news")
def collect_daily_news():
    """Graph to collect news articles for all categories."""
    categories = fetch_categories()
    category_queries = build_all_queries(categories)
    return process_all_categories(category_queries)