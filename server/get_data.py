import requests
import hashlib
import datetime
import time
import os
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# API Keys and Configuration
NEWS_API_KEY = os.getenv('NEWS_API_KEY')

# Initialize Firebase
cred = credentials.Certificate("server/market-sentiment-de-llm-firebase-adminsdk-fbsvc-9b39350acd.json")
firebase_admin.initialize_app(cred)

# Get Firestore client
db = firestore.client()

def generate_content_hash(content):
    """Generate a hash of the article content to avoid duplicates"""
    return hashlib.md5(content.encode('utf-8')).hexdigest()

def fetch_articles(category, query, domains="", count=25):
    """Fetch articles from NewsAPI based on category and query"""
    url = "https://newsapi.org/v2/everything"
    
    # The current date
    today = datetime.datetime.now().strftime('%Y-%m-%d')
    
    # One week ago for articles
    week_ago = (datetime.datetime.now() - datetime.timedelta(days=7)).strftime('%Y-%m-%d')
    
    params = {
        'q': query,
        'apiKey': NEWS_API_KEY,
        'language': 'en',
        'sortBy': 'publishedAt',
        'pageSize': count,
        'from': week_ago,
        'to': today
    }
    
    # Add domains if provided
    if domains:
        params['domains'] = domains
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  # Raise exception for HTTP errors
        
        data = response.json()
        articles = data.get('articles', [])
        
        print(f"Fetched {len(articles)} {category} articles")
        return articles
    except Exception as e:
        print(f"Error fetching {category} articles: {e}")
        return []

def store_article(article, category):
    """Process and store an article in Firestore without storage tier concept"""
    # Extract relevant fields
    title = article.get('title', '')
    content = article.get('content', article.get('description', ''))
    description = article.get('description', '')
    url = article.get('url', '')
    published_at = article.get('publishedAt', '')
    source_name = article.get('source', {}).get('name', 'Unknown')
    
    # Generate content hash to avoid duplicates
    content_hash = generate_content_hash(f"{title}:{content}")
    
    # Check if article already exists
    articles_ref = db.collection('articles')
    query = articles_ref.where('content_hash', '==', content_hash).limit(1)
    existing_docs = query.stream()
    
    if len(list(existing_docs)) > 0:
        print(f"Article already exists: {title}")
        return None
    
    # Convert publishedAt to datetime if it's a string
    if isinstance(published_at, str):
        try:
            published_at = datetime.datetime.strptime(
                published_at, "%Y-%m-%dT%H:%M:%SZ"
            )
        except ValueError:
            # Handle other date formats if needed
            published_at = datetime.datetime.now()
    
    # Prepare article document
    article_doc = {
        "title": title,
        "content": content,
        "description": description,
        "url": url,
        "published_at": published_at,
        "source_name": source_name,
        "category": category,
        "content_hash": content_hash,
        "collection_date": firestore.SERVER_TIMESTAMP,
        "processed": False
    }
    
    # Insert into Firestore
    try:
        # Add with auto-generated ID
        new_article_ref = articles_ref.document()
        new_article_ref.set(article_doc)
        print(f"Stored article: {title}")
        return new_article_ref.id
    except Exception as e:
        print(f"Error storing article: {e}")
        return None

def fetch_categories():
    """Fetch active categories from Firestore"""
    categories_ref = db.collection('categories')
    query = categories_ref.where('active', '==', True)
    
    categories = {}
    for doc in query.stream():
        category_id = doc.id
        category_data = doc.to_dict()
        categories[category_id] = category_data
        
    return categories

def build_category_query(category_id):
    """Build a simple query string for a category based on its topics"""
    # Get active topics for this category
    topics_query = db.collection('topics').where('category_id', '==', category_id).where('active', '==', True)
    topics = list(topics_query.stream())
    
    # Collect keywords from topics
    keywords = [topic_doc.to_dict().get('keyword') for topic_doc in topics if topic_doc.to_dict().get('keyword')]
    
    # Join keywords with OR operators
    if keywords:
        return " OR ".join(keywords)
    else:
        # Fallback queries if no topics/keywords are found
        fallback_queries = {
            'geopolitical': 'geopolitical OR sanctions OR conflict',
            'economic': 'inflation OR GDP OR "interest rates"',
            'corporate': 'earnings OR merger OR acquisition',
            'sentiment': '"market outlook" OR "investor sentiment"'
        }
        return fallback_queries.get(category_id, '')

def collect_daily_news():
    """Main function to collect daily news articles across categories"""
    print(f"Starting news collection: {datetime.datetime.now()}")
    
    # Fetch categories from Firestore
    categories = fetch_categories()
    
    # Track stored articles
    stored_count = 0
    category_stored = {category_id: 0 for category_id in categories}
    stored_article_ids = []
    
    # Process each category
    for category_id, category_data in categories.items():
        # Build dynamic query from topics instead of using static query
        query = build_category_query(category_id)
        target_count = category_data['target_count']
        domains = category_data.get('domains', '')
        
        # Fetch articles
        articles = fetch_articles(category_id, query, domains, count=target_count*2)
        
        # Process and store articles
        for article in articles:
            # Stop if we've reached our target for this category
            if category_stored[category_id] >= target_count:
                break
                
            article_id = store_article(article, category_id)
            if article_id:
                stored_count += 1
                category_stored[category_id] += 1
                stored_article_ids.append(article_id)
        
        # Add delay between API calls to avoid rate limits
        time.sleep(1)
    
    # Log collection summary
    print(f"Collection complete: {stored_count} total articles stored")
    for category, count in category_stored.items():
        target = categories[category]['target_count']
        print(f"  {category}: {count}/{target}")
    
    # Return collection stats
    return {
        "total_collected": stored_count,
        "category_counts": category_stored,
        "article_ids": stored_article_ids,
        "timestamp": firestore.SERVER_TIMESTAMP
    }

def run_daily_collection():
    """Run the collection process and handle any errors"""
    try:
        stats = collect_daily_news()
        
        # Store collection stats
        collection_date = datetime.datetime.now().strftime('%Y-%m-%d')
        stats_ref = db.collection('collection_stats').document(collection_date)
        stats_ref.set(stats)
        
        # Print completion message
        print(f"Daily collection completed successfully at {datetime.datetime.now()}")
        print(f"Total articles collected: {stats['total_collected']}")
        
        return stats
    except Exception as e:
        error_message = f"Error during collection: {e}"
        print(error_message)
        
        # Store error information
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
        error_ref = db.collection('collection_errors').document(timestamp)
        error_ref.set({
            "error": str(e),
            "timestamp": firestore.SERVER_TIMESTAMP
        })
        
        return {"error": str(e)}

if __name__ == "__main__":
    run_daily_collection()