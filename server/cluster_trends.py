import firebase_admin
from firebase_admin import credentials, firestore
import datetime
from sklearn.decomposition import PCA
import nltk
import logging
import traceback
import torch

logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

FINBERT_AVAILABLE = False
SUMMARIZER_AVAILABLE = False

try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt')

# Initialize Firebase (if not already initialized)
import firebase_admin
from firebase_admin import credentials, firestore
if not firebase_admin._apps:
    try:
        cred = credentials.Certificate("server/market-sentiment-de-llm-firebase-adminsdk-fbsvc-9b39350acd.json")
        firebase_admin.initialize_app(cred)
        logger.info("Firebase initialized successfully")
    except Exception as e:
        logger.error(f"Firebase initialization error: {e}")
        raise

db = firestore.client()

# Load FinBERT for sentiment
try:
    from transformers import AutoModelForSequenceClassification, AutoTokenizer
    import torch

    sentiment_tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert")
    sentiment_model = AutoModelForSequenceClassification.from_pretrained("ProsusAI/finbert")
    FINBERT_AVAILABLE = True
    logger.info("FinBERT model loaded successfully")
except Exception as e:
    logger.warning(f"FinBERT model loading failed: {e}. Will use basic sentiment analysis.")

# Load BART summarizer
try:
    from transformers import pipeline
    summarizer = pipeline("summarization", model="facebook/bart-large-cnn")
    SUMMARIZER_AVAILABLE = True
    logger.info("BART summarization model loaded successfully")
except Exception as e:
    logger.warning(f"BART model loading failed: {e}. Will use extractive summarization.")

def mean_pooling(model_output, attention_mask):
    """
    Mean pooling: aggregate the token-level embeddings to produce a single vector,
    ignoring padding tokens.
    """
    token_embeddings = model_output.last_hidden_state  # [batch_size, seq_len, hidden_dim]
    input_mask_expanded = attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
    
    # Sum the token embeddings (excluding padding) and divide by the actual token count
    sum_embeddings = torch.sum(token_embeddings * input_mask_expanded, dim=1)
    sum_mask = torch.clamp(input_mask_expanded.sum(dim=1), min=1e-9)
    return sum_embeddings / sum_mask


def vectorize_articles(articles, use_finbert=True):
    """
    Convert articles to vector representations using FinBERT or TF-IDF.
    Now uses mean pooling for more relevant embeddings from BERT-like models.
    """
    global FINBERT_AVAILABLE
    
    if use_finbert and FINBERT_AVAILABLE:
        try:
            logger.info("Vectorizing articles using FinBERT with mean pooling.")
            from transformers import AutoModel
            import numpy as np

            # Reuse the existing FinBERT tokenizer and get the base model
            tokenizer = sentiment_tokenizer
            base_model = AutoModel.from_pretrained("ProsusAI/finbert")
            
            article_vectors = []
            article_ids = []
            
            for article in articles:
                article_id = article.get('id')
                title = article.get('title', '')
                description = article.get('description', '')
                content = article.get('content', '')
                
                # Combine title + partial content for embedding
                if len(content) > 1000:
                    article_text = f"{title}. {description} {content[:1000]}"
                else:
                    article_text = f"{title}. {description} {content}"
                
                # Tokenize
                inputs = tokenizer(
                    article_text,
                    return_tensors="pt",
                    truncation=True,
                    max_length=512,
                    padding="max_length"
                )
                
                with torch.no_grad():
                    outputs = base_model(**inputs)
                
                # Mean pooling over all tokens (ignoring padding)
                pooled_embedding = mean_pooling(outputs, inputs['attention_mask'])
                
                # Convert to numpy
                vector = pooled_embedding.squeeze().cpu().numpy()
                
                # Optional: L2 normalize the embedding (often helps clustering)
                norm = np.linalg.norm(vector)
                if norm > 0:
                    vector = vector / norm
                
                article_vectors.append(vector)
                article_ids.append(article_id)
            
            return np.array(article_vectors), article_ids
        
        except Exception as e:
            logger.error(f"FinBERT vectorization failed: {e}")
            logger.error(traceback.format_exc())
            FINBERT_AVAILABLE = False
    
    # If FinBERT not available or fails, fallback (TF-IDF or empty)
    logger.error("No available vectorization method. Returning empty vectors.")
    return [], []

def reduce_dimensions(article_vectors, n_components=50):
    """Apply PCA (or UMAP) to reduce dimensionality before clustering."""
    pca = PCA(n_components=n_components, random_state=42)
    reduced = pca.fit_transform(article_vectors)
    return reduced
    
def cluster_articles(article_vectors, article_ids, method='kmeans', n_clusters=None):
    """
    Cluster article vectors using various algorithms.
    
    Args:
        article_vectors: Numerical vectors representing articles
        article_ids: IDs corresponding to each vector
        method: Clustering algorithm to use ('kmeans', 'dbscan', or 'agglomerative')
        n_clusters: Number of clusters (auto-determined if None)
    
    Returns:
        Dict mapping article_ids to cluster assignments
    """
    try:
        import numpy as np
        
        if len(article_vectors) < 2:
            logger.warning("Not enough articles to cluster")
            return {}
            
        # Try to import scikit-learn
        try:
            from sklearn.cluster import KMeans, DBSCAN, AgglomerativeClustering
        except ImportError:
            logger.error("scikit-learn not available, cannot perform clustering")
            return {}
        
        # If n_clusters not specified, estimate using silhouette score
        if n_clusters is None:
            try:
                from sklearn.metrics import silhouette_score
                
                # Try a range of cluster counts, selecting the best
                best_score = -1
                best_n = 2  # Default minimum

                max_n = min(50, len(article_vectors) // 2)  # allow more clusters if data is large

                
                for n in range(2, max_n + 1):
                    if n >= len(article_vectors):
                        break
                        
                    km = KMeans(n_clusters=n, random_state=42, n_init=10)
                    labels = km.fit_predict(article_vectors)
                    
                    if len(set(labels)) <= 1:  # Only one cluster found
                        continue
                        
                    score = silhouette_score(article_vectors, labels)
                    logger.info(f"Silhouette score for {n} clusters: {score:.4f}")
                    if score > best_score:
                        best_score = score
                        best_n = n
                
                n_clusters = best_n
                logger.info(f"Automatically determined optimal clusters: {n_clusters}")
            except Exception as e:
                logger.warning(f"Error determining optimal clusters: {e}")
                n_clusters = min(10, len(article_vectors) // 10)  # Fallback
        
        # Choose clustering algorithm
        if method == 'kmeans':
            clustering = KMeans(n_clusters=n_clusters, random_state=42, n_init=10).fit(article_vectors)
            labels = clustering.labels_
            logger.info(f"KMeans clustering complete with {n_clusters} clusters")
        elif method == 'dbscan':
            # Epsilon determined by nearest neighbor distances
            from sklearn.neighbors import NearestNeighbors
            nn = NearestNeighbors(n_neighbors=2)
            nn.fit(article_vectors)
            distances, indices = nn.kneighbors(article_vectors)
            distances = np.sort(distances[:, 1])
            epsilon = np.percentile(distances, 90)  # Use 90th percentile
            
            clustering = DBSCAN(eps=epsilon, min_samples=2).fit(article_vectors)
            labels = clustering.labels_
            logger.info(f"DBSCAN clustering complete with epsilon={epsilon:.4f}")
        else:  # default to agglomerative
            clustering = AgglomerativeClustering(n_clusters=n_clusters).fit(article_vectors)
            labels = clustering.labels_
            logger.info(f"Agglomerative clustering complete with {n_clusters} clusters")
        
        # Create a mapping from article ID to cluster
        clusters = {}
        for article_id, label in zip(article_ids, labels):
            clusters[article_id] = int(label)
        
        # Log cluster distribution
        cluster_counts = {}
        for label in labels:
            label = int(label)
            if label not in cluster_counts:
                cluster_counts[label] = 0
            cluster_counts[label] += 1
            
        for cluster_id, count in sorted(cluster_counts.items()):
            logger.info(f"Cluster {cluster_id}: {count} articles")
            
        return clusters
        
    except Exception as e:
        logger.error(f"Clustering failed: {e}")
        logger.error(traceback.format_exc())
        return {}


def extract_cluster_keywords(articles, clusters, top_n=10):
    """
    Extract the most representative keywords for each cluster.
    
    Args:
        articles: List of article dictionaries
        clusters: Dict mapping article_ids to cluster IDs
        top_n: Number of top keywords to extract per cluster
        
    Returns:
        Dict mapping cluster IDs to lists of keywords
    """
    try:
        from sklearn.feature_extraction.text import TfidfVectorizer
        import numpy as np
        
        # Group articles by cluster
        cluster_articles = {}
        for article in articles:
            article_id = article.get('id')
            cluster_id = clusters.get(article_id, -1)
            if cluster_id == -1:  # Skip unclustered articles
                continue
                
            if cluster_id not in cluster_articles:
                cluster_articles[cluster_id] = []
            cluster_articles[cluster_id].append(article)
        
        # For each cluster, find distinctive terms
        cluster_keywords = {}
        
        for cluster_id, cluster_docs in cluster_articles.items():
            # Create texts for this cluster
            cluster_texts = []
            for article in cluster_docs:
                title = article.get('title', '')
                description = article.get('description', '')
                content = article.get('content', '')
                
                cluster_texts.append(f"{title}. {description} {content}")
            
            # Get all other articles not in this cluster
            other_texts = []
            for other_id, other_docs in cluster_articles.items():
                if other_id != cluster_id:
                    for article in other_docs:
                        title = article.get('title', '')
                        description = article.get('description', '')
                        content = article.get('content', '')
                        
                        other_texts.append(f"{title}. {description} {content}")
            
            # If we have too few articles in a cluster, skip complex analysis
            if len(cluster_texts) < 2:
                # Just use simple frequency for very small clusters
                text = " ".join(cluster_texts)
                words = text.lower().split()
                word_freq = {}
                for word in words:
                    if len(word) > 3:  # Filter out short words
                        word_freq[word] = word_freq.get(word, 0) + 1
                
                # Filter out stopwords
                stopwords = {"the", "and", "a", "to", "of", "in", "that", "for", "is", "on",
                             "with", "by", "at", "as", "an", "are", "from", "it", "was", "be"}
                keywords = [(word, freq) for word, freq in word_freq.items() if word not in stopwords]
                
                keywords.sort(key=lambda x: x[1], reverse=True)
                cluster_keywords[cluster_id] = [k[0] for k in keywords[:top_n]]
                continue
            
            # Use TF-IDF to find distinctive terms
            vectorizer = TfidfVectorizer(
                max_features=1000,
                stop_words='english',
                ngram_range=(1, 2)
            )
            
            # Fit on all documents
            all_texts = cluster_texts + other_texts
            tfidf_matrix = vectorizer.fit_transform(all_texts)
            
            # Get average TF-IDF scores for this cluster vs. others
            cluster_tfidf = tfidf_matrix[:len(cluster_texts)].toarray().mean(axis=0)
            if other_texts:
                other_tfidf = tfidf_matrix[len(cluster_texts):].toarray().mean(axis=0)
            else:
                other_tfidf = np.zeros_like(cluster_tfidf)
            
            # Find terms that are distinctive to this cluster
            distinctiveness = cluster_tfidf - other_tfidf
            
            # Get feature names
            feature_names = vectorizer.get_feature_names_out()
            
            # Sort by distinctiveness
            sorted_indices = np.argsort(distinctiveness)[::-1]
            top_indices = sorted_indices[:top_n]
            
            # Get top keywords
            cluster_keywords[cluster_id] = [feature_names[i] for i in top_indices]
        
        return cluster_keywords
        
    except Exception as e:
        logger.error(f"Cluster keyword extraction failed: {e}")
        logger.error(traceback.format_exc())
        return {}

def analyze_articles_with_clustering(limit=500, n_clusters=None, method='kmeans'):
    """
    Analyze articles using clustering approach to identify trends.
    
    Args:
        limit: Maximum number of articles to process
        n_clusters: Number of clusters (auto-determined if None)
        method: Clustering method ('kmeans', 'dbscan', 'agglomerative')
        
    Returns:
        List of trend dictionaries
    """
    try:
        # Fetch recent articles
        articles_ref = db.collection('articles')
        query = articles_ref.order_by('published_at', direction=firestore.Query.DESCENDING).limit(limit)
        articles = []
        article_docs = list(query.stream())
        
        for doc in article_docs:
            article = doc.to_dict()
            article['id'] = doc.id
            articles.append(article)
        
        # If we didn't get enough articles, add some unprocessed ones
        if len(articles) < limit:
            remaining = limit - len(articles)
            unprocessed_query = articles_ref.where('processed', '==', False).limit(remaining)
            for doc in unprocessed_query.stream():
                article = doc.to_dict()
                article['id'] = doc.id
                articles.append(article)
        
        logger.info(f"Retrieved {len(articles)} articles for clustering analysis")
        
        # Vectorize articles
        article_vectors, article_ids = vectorize_articles(articles)
        
        if len(article_vectors) == 0:
            logger.error("Failed to vectorize articles")
            return []
        
        # Optionally reduce dimensions for better clustering
        if len(article_vectors) > 0 and article_vectors.shape[1] > 100:
            article_vectors = reduce_dimensions(article_vectors, n_components=min(50, len(article_vectors)-1))
        
        # Cluster articles
        clusters = cluster_articles(article_vectors, article_ids, method=method, n_clusters=n_clusters)
        
        if not clusters:
            logger.error("Clustering failed")
            return []
        
        # Extract keywords for each cluster
        cluster_keywords = extract_cluster_keywords(articles, clusters)
        
        # Count articles in each cluster
        cluster_data = {}
        for article in articles:
            article_id = article.get('id')
            if article_id not in clusters:
                continue
                
            cluster_id = clusters[article_id]
            
            if cluster_id not in cluster_data:
                cluster_data[cluster_id] = {
                    'article_count': 0,
                    'articles': []
                }
            
            cluster_data[cluster_id]['article_count'] += 1
            cluster_data[cluster_id]['articles'].append(article_id)
        
        # Prepare trend data
        trends = []
        for cluster_id, data in cluster_data.items():
            # Skip small clusters
            if data['article_count'] < 2:
                continue
            
            trend = {
                'cluster_id': cluster_id,
                'article_count': data['article_count'],
                'keywords': cluster_keywords.get(cluster_id, []),
                'articles': data['articles']
            }
            trends.append(trend)
        
        # Sort by article count (popularity)
        trends.sort(key=lambda x: x['article_count'], reverse=True)
        
        # Store trends in Firestore
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d')
        trends_ref = db.collection('clustered_trends').document(timestamp)
        trends_ref.set({
            'date': timestamp,
            'trends': trends,
            'total_articles': len(articles),
            'method': method,
            'n_clusters': n_clusters,
            'generated_at': firestore.SERVER_TIMESTAMP
        })
        
        logger.info(f"Stored {len(trends)} clustered trends for {timestamp}")
        
        return trends
        
    except Exception as e:
        logger.error(f"Clustering analysis failed: {e}")
        logger.error(traceback.format_exc())
        return []

def run_clustering_analysis(limit=500, n_clusters=None, method='kmeans'):
    """
    Run the clustering-based trend analysis and print top trends.
    
    Args:
        limit: Maximum number of articles to process
        n_clusters: Number of clusters (auto-determined if None)
        method: Clustering method ('kmeans', 'dbscan', 'agglomerative')
        
    Returns:
        List of trend dictionaries
    """
    logger.info("Starting clustering-based trend analysis")
    trends = analyze_articles_with_clustering(limit=limit, n_clusters=n_clusters, method=method)
    
    # Print top trends
    logger.info("Top clustered trends:")
    for i, trend in enumerate(trends[:5]):
        keywords_str = ", ".join(trend['keywords'][:5])
        logger.info(f"Trend {i+1}: {keywords_str} - {trend['article_count']} articles")
    
    return trends

def update_trends_collection_from_clusters(trends, period="daily"):
    """
    Update the main trends collection with the results from clustering.
    This makes the clusters accessible through the same API as the old keyword trends.
    
    Args:
        trends: List of trend dictionaries from clustering
        period: The time period ('daily', 'weekly', 'monthly')
        
    Returns:
        Number of trends added to the trends collection
    """
    try:
        import datetime
        
        if not trends:
            logger.warning(f"No {period} trends to add to trends collection")
            return 0
            
        today = datetime.datetime.now()
        date_str = today.strftime('%Y-%m-%d')
        
        # Get existing trends reference
        trends_ref = db.collection('trends')
        
        # Track how many we add
        added_count = 0
        
        # Process each cluster as a trend
        for trend in trends:
            # Create a unique keyword identifier based on period and cluster
            cluster_id = trend.get('cluster_id', 'unknown')
            
            # Get the first 3 keywords as the trend name
            keywords = trend.get('keywords', [])
            if keywords:
                trend_name = " ".join(keywords[:3]).lower()
            else:
                # If no keywords are available, use the cluster ID
                trend_name = f"cluster_{cluster_id}"
            
            # Create a unique identifier that won't conflict with regular keyword trends
            trend_key = f"cluster_{period}_{cluster_id}"
            
            # Check if we already have this trend
            query = trends_ref.where('keyword', '==', trend_key)
            existing = list(query.stream())
            
            # Get article IDs
            article_ids = trend.get('articles', [])
            
            if existing:
                # Update existing trend
                existing_ref = existing[0].reference
                existing_ref.update({
                    "frequency": len(article_ids),
                    "articles": article_ids,  # Replace with the full current set
                    "last_seen": date_str,
                    "keywords": trend.get('keywords', []),  # Add the cluster's keywords
                    "updated_at": firestore.SERVER_TIMESTAMP
                })
            else:
                # Create a new trend
                new_trend = {
                    "keyword": trend_key,           # Unique identifier
                    "display_name": trend_name,     # Human-readable name
                    "frequency": len(article_ids),  # How many articles in this cluster
                    "first_seen": date_str,
                    "last_seen": date_str,
                    "articles": article_ids,
                    "cluster_id": cluster_id,
                    "period": period,
                    "keywords": trend.get('keywords', []),
                    "created_at": firestore.SERVER_TIMESTAMP,
                    "updated_at": firestore.SERVER_TIMESTAMP
                }
                
                # Add additional metadata based on period
                if period in ['weekly', 'monthly']:
                    new_trend['date_distribution'] = trend.get('date_distribution', {})
                    new_trend['trend_direction'] = trend.get('trend_direction', 'stable')
                    
                if period == 'monthly':
                    new_trend['week_distribution'] = trend.get('week_distribution', {})
                    new_trend['weeks_present'] = trend.get('weeks_present', 1)
                
                # Add the trend to the collection
                trends_ref.add(new_trend)
                added_count += 1
        
        logger.info(f"Added/updated {added_count} {period} trends to the trends collection")
        return added_count
        
    except Exception as e:
        logger.error(f"Error updating trends collection with {period} clusters: {e}")
        logger.error(traceback.format_exc())
        return 0
def get_daily_clustered_trends(method='kmeans', n_clusters=None, limit=200):
    """
    Get trends for the current day using clustering approach.
    Stores all trends in a single document with ID format: daily_{YYYY-MM-DD}
    """
    try:
        import datetime
        
        # Calculate date range for today
        today = datetime.datetime.now()
        today_str = today.strftime('%Y-%m-%d')
        today_start = datetime.datetime(today.year, today.month, today.day, 0, 0, 0)
        tomorrow_start = today_start + datetime.timedelta(days=1)
        
        logger.info(f"Getting daily trends for {today_str}")
        
        # Query articles from today
        articles_ref = db.collection('articles')
        query = articles_ref.where("published_at", ">=", today_start).where("published_at", "<", tomorrow_start).limit(limit)
        
        articles = []
        for doc in query.stream():
            article = doc.to_dict()
            article['id'] = doc.id
            articles.append(article)
        
        logger.info(f"Found {len(articles)} articles published today")
        
        # If we didn't get enough articles from today, add some from yesterday
        if len(articles) < 20:  # Need a minimum number for meaningful clustering
            yesterday_start = today_start - datetime.timedelta(days=1)
            
            yesterday_query = articles_ref.where("published_at", ">=", yesterday_start).where("published_at", "<", today_start).limit(limit - len(articles))
            
            yesterday_articles = []
            for doc in yesterday_query.stream():
                article = doc.to_dict()
                article['id'] = doc.id
                yesterday_articles.append(article)
                
            logger.info(f"Added {len(yesterday_articles)} articles from yesterday")
            articles.extend(yesterday_articles)
                               
        logger.info(f"Retrieved {len(articles)} articles for daily trend clustering")
        
        if len(articles) < 5:
            logger.warning("Not enough articles for meaningful clustering")
            return []
        
        # Vectorize and cluster
        article_vectors, article_ids = vectorize_articles(articles)
        
        if len(article_vectors) == 0:
            logger.error("Failed to vectorize articles for daily trends")
            return []
        
        # Use a reasonable number of clusters for daily analysis
        if n_clusters is None:
            n_clusters = min(20, max(5, len(articles) // 10))
        
        clusters = cluster_articles(article_vectors, article_ids, method=method, n_clusters=n_clusters)
        
        if not clusters:
            logger.error("Clustering failed for daily trends")
            return []
        
        # Extract keywords and summaries
        cluster_keywords = extract_cluster_keywords(articles, clusters)
        
        # Count articles in each cluster
        cluster_data = {}
        for article in articles:
            article_id = article.get('id')
            if article_id not in clusters:
                continue
                
            cluster_id = clusters[article_id]
            
            if cluster_id not in cluster_data:
                cluster_data[cluster_id] = {
                    'article_count': 0,
                    'articles': []
                }
            
            cluster_data[cluster_id]['article_count'] += 1
            cluster_data[cluster_id]['articles'].append(article_id)
        
        # Prepare trend data
        trends = []
        for cluster_id, data in cluster_data.items():
            # Skip clusters with only one article
            if data['article_count'] < 2:
                continue
            
            trend = {
                'cluster_id': cluster_id,
                'article_count': data['article_count'],
                'keywords': cluster_keywords.get(cluster_id, []),
                'articles': data['articles'],
                'period': 'daily'
            }
            trends.append(trend)
        
        # Sort by article count
        trends.sort(key=lambda x: x['article_count'], reverse=True)
        
        # Store all daily trends in a single document with ID format: daily_{YYYY-MM-DD}
        daily_doc_id = f"daily_{today_str}"
        trends_ref = db.collection('clustered_trends').document(daily_doc_id)
        trends_ref.set({
            'date': today_str,
            'trends': trends,
            'period': 'daily',
            'total_articles': len(articles),
            'method': method,
            'n_clusters': n_clusters,
            'generated_at': firestore.SERVER_TIMESTAMP
        })
        
        logger.info(f"Stored {len(trends)} daily trends as '{daily_doc_id}'")
        
        return trends
        
    except Exception as e:
        logger.error(f"Daily clustering analysis failed: {e}")
        logger.error(traceback.format_exc())
        return []

def get_weekly_clustered_trends(method='kmeans', n_clusters=None, limit=500):
    """
    Get trends for the current week using clustering approach.
    Stores all trends in a single document with ID format: weekly_{YYYY}_W{week_number}
    """
    try:
        import datetime
        
        # Calculate date range for current week (Monday to today)
        today = datetime.datetime.now()
        start_of_week = today - datetime.timedelta(days=today.weekday())
        start_of_week = datetime.datetime(start_of_week.year, start_of_week.month, start_of_week.day, 0, 0, 0)
        end_of_today = datetime.datetime(today.year, today.month, today.day, 23, 59, 59)
        
        # Get week number for document ID
        week_num = today.isocalendar()[1]
        week_doc_id = f"weekly_{today.year}_W{week_num}"
        
        logger.info(f"Getting weekly trends from {start_of_week.strftime('%Y-%m-%d')} to {end_of_today.strftime('%Y-%m-%d')}")
        
        # Query articles from this week
        articles_ref = db.collection('articles')
        query = articles_ref.where("published_at", ">=", start_of_week).where("published_at", "<=", end_of_today).limit(limit)
        
        articles = []
        for doc in query.stream():
            article = doc.to_dict()
            article['id'] = doc.id
            articles.append(article)
        
        logger.info(f"Found {len(articles)} articles published this week")
        
        # If not enough articles, get more from the previous week
        if len(articles) < 20:
            prev_week_start = start_of_week - datetime.timedelta(days=7)
            
            prev_week_query = articles_ref.where("published_at", ">=", prev_week_start).where("published_at", "<", start_of_week).limit(limit - len(articles))
            
            prev_week_articles = []
            for doc in prev_week_query.stream():
                article = doc.to_dict()
                article['id'] = doc.id
                prev_week_articles.append(article)
            
            logger.info(f"Added {len(prev_week_articles)} articles from previous week")
            articles.extend(prev_week_articles)
        
        logger.info(f"Retrieved {len(articles)} articles for weekly trend clustering")
        
        if len(articles) < 10:
            logger.warning("Not enough articles for meaningful clustering")
            return []
        
        # Vectorize and cluster
        article_vectors, article_ids = vectorize_articles(articles)
        
        if len(article_vectors) == 0:
            logger.error("Failed to vectorize articles for weekly trends")
            return []
        
        # Use a reasonable number of clusters for weekly analysis
        if n_clusters is None:
            n_clusters = min(15, max(10, len(articles) // 20))
        
        clusters = cluster_articles(article_vectors, article_ids, method=method, n_clusters=n_clusters)
        
        if not clusters:
            logger.error("Clustering failed for weekly trends")
            return []
        
        # Extract keywords and summaries
        cluster_keywords = extract_cluster_keywords(articles, clusters)
        
        # Count articles in each cluster
        cluster_data = {}
        for article in articles:
            article_id = article.get('id')
            if article_id not in clusters:
                continue
                
            cluster_id = clusters[article_id]
            
            if cluster_id not in cluster_data:
                cluster_data[cluster_id] = {
                    'article_count': 0,
                    'articles': [],
                    'date_counts': {}  # Track article distribution by day
                }
            
            cluster_data[cluster_id]['article_count'] += 1
            cluster_data[cluster_id]['articles'].append(article_id)
                
            # Track distribution by day
            pub_date = article.get('published_at')
            if pub_date:
                # Convert to date string
                day_key = pub_date.strftime('%Y-%m-%d') if hasattr(pub_date, 'strftime') else str(pub_date)[:10]
                if day_key not in cluster_data[cluster_id]['date_counts']:
                    cluster_data[cluster_id]['date_counts'][day_key] = 0
                cluster_data[cluster_id]['date_counts'][day_key] += 1
        
        # Prepare trend data
        trends = []
        for cluster_id, data in cluster_data.items():
            # Skip clusters with only one article or appearing on only one day
            if data['article_count'] < 3 or len(data['date_counts']) < 2:
                continue
            
            # Calculate trend direction (are articles increasing or decreasing over time?)
            dates = sorted(data['date_counts'].keys())
            if len(dates) >= 2:
                first_day_count = data['date_counts'][dates[0]]
                last_day_count = data['date_counts'][dates[-1]]
                trend_direction = "increasing" if last_day_count >= first_day_count else "decreasing"
            else:
                trend_direction = "stable"
            
            trend = {
                'cluster_id': cluster_id,
                'article_count': data['article_count'],
                'keywords': cluster_keywords.get(cluster_id, []),
                'articles': data['articles'],
                'date_distribution': data['date_counts'],
                'trend_direction': trend_direction,
                'period': 'weekly',
                'days_present': len(data['date_counts'])
            }
            trends.append(trend)
        
        # Sort by days present (more days = more persistent trend) then by article count
        trends.sort(key=lambda x: (x['days_present'], x['article_count']), reverse=True)
        
        # Store all weekly trends in a single document with ID format: weekly_{YYYY}_W{week_number}
        trends_ref = db.collection('clustered_trends').document(week_doc_id)
        trends_ref.set({
            'date_range': {
                'start': start_of_week.strftime('%Y-%m-%d'),
                'end': end_of_today.strftime('%Y-%m-%d')
            },
            'trends': trends,
            'period': 'weekly',
            'week_number': week_num,
            'year': today.year,
            'total_articles': len(articles),
            'method': method,
            'n_clusters': n_clusters,
            'generated_at': firestore.SERVER_TIMESTAMP
        })
        
        logger.info(f"Stored {len(trends)} weekly trends as '{week_doc_id}'")
        
        return trends
        
    except Exception as e:
        logger.error(f"Weekly clustering analysis failed: {e}")
        logger.error(traceback.format_exc())
        return []

def get_monthly_clustered_trends(method='kmeans', n_clusters=None, limit=1000):
    """
    Get trends for the current month using clustering approach.
    Stores all trends in a single document with ID format: monthly_{YYYY}_{MM}
    """
    try:
        import datetime
        
        # Calculate date range for current month
        today = datetime.datetime.now()
        start_of_month = datetime.datetime(today.year, today.month, 1, 0, 0, 0)
        end_of_today = datetime.datetime(today.year, today.month, today.day, 23, 59, 59)
        
        # Document ID format: monthly_{YYYY}_{MM}
        month_doc_id = f"monthly_{today.year}_{today.month:02d}"
        
        logger.info(f"Getting monthly trends from {start_of_month.strftime('%Y-%m-%d')} to {end_of_today.strftime('%Y-%m-%d')}")
        
        # Query articles from this month
        articles_ref = db.collection('articles')
        query = articles_ref.where("published_at", ">=", start_of_month).where("published_at", "<=", end_of_today).limit(limit)
        
        articles = []
        for doc in query.stream():
            article = doc.to_dict()
            article['id'] = doc.id
            articles.append(article)
        
        logger.info(f"Found {len(articles)} articles published this month")
        
        # If not enough articles, get more from the previous month
        if len(articles) < 30:
            # Calculate previous month
            if start_of_month.month == 1:
                prev_month_start = datetime.datetime(start_of_month.year - 1, 12, 1, 0, 0, 0)
            else:
                prev_month_start = datetime.datetime(start_of_month.year, start_of_month.month - 1, 1, 0, 0, 0)
            
            prev_month_query = articles_ref.where("published_at", ">=", prev_month_start).where("published_at", "<", start_of_month).limit(limit - len(articles))
            
            prev_month_articles = []
            for doc in prev_month_query.stream():
                article = doc.to_dict()
                article['id'] = doc.id
                prev_month_articles.append(article)
            
            logger.info(f"Added {len(prev_month_articles)} articles from previous month")
            articles.extend(prev_month_articles)
        
        logger.info(f"Retrieved {len(articles)} articles for monthly trend clustering")
        
        if len(articles) < 15:
            logger.warning("Not enough articles for meaningful monthly clustering")
            return []
        
        # Vectorize and cluster
        article_vectors, article_ids = vectorize_articles(articles)
        
        if len(article_vectors) == 0:
            logger.error("Failed to vectorize articles for monthly trends")
            return []
        
        # Use a reasonable number of clusters for monthly analysis (more than weekly)
        if n_clusters is None:
            n_clusters = min(25, max(20, len(articles) // 30))

        article_vectors = reduce_dimensions(article_vectors, n_components=50)
        
        clusters = cluster_articles(article_vectors, article_ids, method=method, n_clusters=n_clusters)
        
        if not clusters:
            logger.error("Clustering failed for monthly trends")
            return []
        
        # Extract keywords and summaries
        cluster_keywords = extract_cluster_keywords(articles, clusters)
        
        # Count articles in each cluster
        cluster_data = {}
        for article in articles:
            article_id = article.get('id')
            if article_id not in clusters:
                continue
                
            cluster_id = clusters[article_id]
            
            if cluster_id not in cluster_data:
                cluster_data[cluster_id] = {
                    'article_count': 0,
                    'articles': [],
                    'date_counts': {},   # Track article distribution by day
                    'week_counts': {}    # Track article distribution by week
                }
            
            cluster_data[cluster_id]['article_count'] += 1
            cluster_data[cluster_id]['articles'].append(article_id)
                
            # Track distribution by day
            pub_date = article.get('published_at')
            if pub_date:
                # Convert to date string
                day_key = pub_date.strftime('%Y-%m-%d') if hasattr(pub_date, 'strftime') else str(pub_date)[:10]
                if day_key not in cluster_data[cluster_id]['date_counts']:
                    cluster_data[cluster_id]['date_counts'][day_key] = 0
                cluster_data[cluster_id]['date_counts'][day_key] += 1
                
                # Track distribution by week
                if hasattr(pub_date, 'isocalendar'):
                    week_num = pub_date.isocalendar()[1]
                    week_key = f"{pub_date.year}-W{week_num}"
                else:
                    # Fallback if not a datetime object
                    # This is approximate and might not be accurate if the string date crosses week boundaries
                    try:
                        dt = datetime.datetime.strptime(str(pub_date)[:10], '%Y-%m-%d')
                        week_num = dt.isocalendar()[1]
                        week_key = f"{dt.year}-W{week_num}"
                    except:
                        # Last resort fallback
                        week_key = day_key[:-3]  # Use year-month as fallback for week
                        
                if week_key not in cluster_data[cluster_id]['week_counts']:
                    cluster_data[cluster_id]['week_counts'][week_key] = 0
                cluster_data[cluster_id]['week_counts'][week_key] += 1
        
        # Prepare trend data
        trends = []
        for cluster_id, data in cluster_data.items():
            # Skip clusters with few articles or appearing in only one week
            if data['article_count'] < 5 or len(data['week_counts']) < 2:
                continue
            
            # Calculate trend strength
            weeks = sorted(data['week_counts'].keys())
            if len(weeks) >= 2:
                first_week_count = data['week_counts'][weeks[0]]
                last_week_count = data['week_counts'][weeks[-1]]
                middle_weeks_avg = 0
                if len(weeks) > 2:
                    middle_weeks_avg = sum(data['week_counts'][w] for w in weeks[1:-1]) / (len(weeks) - 2)
                else:
                    middle_weeks_avg = (first_week_count + last_week_count) / 2
                
                if last_week_count > middle_weeks_avg:
                    trend_direction = "increasing"
                elif last_week_count < middle_weeks_avg:
                    trend_direction = "decreasing"
                else:
                    trend_direction = "stable"
            else:
                trend_direction = "stable"
            
            trend = {
                'cluster_id': cluster_id,
                'article_count': data['article_count'],
                'keywords': cluster_keywords.get(cluster_id, []),
                'articles': data['articles'],
                'date_distribution': data['date_counts'],
                'week_distribution': data['week_counts'],
                'trend_direction': trend_direction,
                'period': 'monthly',
                'weeks_present': len(data['week_counts']),
                'days_present': len(data['date_counts'])
            }
            trends.append(trend)
        
        # Sort by weeks present (more weeks = more persistent trend) then by article count
        trends.sort(key=lambda x: (x['weeks_present'], x['article_count']), reverse=True)
        
        # Store all monthly trends in a single document with ID format: monthly_{YYYY}_{MM}
        trends_ref = db.collection('clustered_trends').document(month_doc_id)
        trends_ref.set({
            'date_range': {
                'start': start_of_month.strftime('%Y-%m-%d'),
                'end': end_of_today.strftime('%Y-%m-%d')
            },
            'trends': trends,
            'period': 'monthly',
            'month': today.month,
            'month_name': start_of_month.strftime('%B'),  # Month name (e.g., "March")
            'year': today.year,
            'total_articles': len(articles),
            'method': method,
            'n_clusters': n_clusters,
            'generated_at': firestore.SERVER_TIMESTAMP
        })
        
        logger.info(f"Stored {len(trends)} monthly trends as '{month_doc_id}'")

        return trends
        
    except Exception as e:
        logger.error(f"Monthly clustering analysis failed: {e}")
        logger.error(traceback.format_exc())
        return []

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Cluster articles to identify trends')
    parser.add_argument('--limit', type=int, default=500, help='Max number of articles to process')
    parser.add_argument('--method', type=str, default='kmeans', choices=['kmeans', 'dbscan', 'agglomerative'], 
                      help='Clustering method to use')
    parser.add_argument('--clusters', type=int, default=None, help='Number of clusters (auto if not specified)')
    parser.add_argument('--daily', action='store_true', help='Get daily trends')
    parser.add_argument('--weekly', action='store_true', help='Get weekly trends')
    parser.add_argument('--monthly', action='store_true', help='Get monthly trends')
    parser.add_argument('--all', action='store_true', help='Run all time-based clustering (daily, weekly, monthly)')
    parser.add_argument('--general', action='store_true', help='Run general clustering analysis')

    args = parser.parse_args()
    
    # General clustering analysis
    if args.general:
        cluster_trends = run_clustering_analysis(limit=args.limit, n_clusters=args.clusters, method=args.method)
        print(f"Top general clustered trends:")
        for i, trend in enumerate(cluster_trends[:5]):
            keywords = ", ".join(trend['keywords'][:3])
            print(f"{i+1}. {keywords} - {trend['article_count']} articles")
    
    # Time-based trend analyses using clustering
    if args.daily or args.all:
        daily_trends = get_daily_clustered_trends(method=args.method, n_clusters=args.clusters, limit=args.limit)
        print(f"Top daily trends:")
        for i, trend in enumerate(daily_trends[:5]):
            keywords = ", ".join(trend['keywords'][:3])
            print(f"{i+1}. {keywords} - {trend['article_count']} articles")
            
    if args.weekly or args.all:
        weekly_trends = get_weekly_clustered_trends(method=args.method, n_clusters=args.clusters, limit=args.limit)
        print(f"Top weekly trends:")
        for i, trend in enumerate(weekly_trends[:5]):
            keywords = ", ".join(trend['keywords'][:3])
            print(f"{i+1}. {keywords} - {trend['article_count']} articles")
        
    if args.monthly or args.all:
        monthly_trends = get_monthly_clustered_trends(method=args.method, n_clusters=args.clusters, limit=args.limit)
        print(f"Top monthly trends:")
        for i, trend in enumerate(monthly_trends[:5]):
            keywords = ", ".join(trend['keywords'][:3])
            print(f"{i+1}. {keywords} - {trend['article_count']} articles")

    # If no specific flags, run daily trends
    if not (args.daily or args.weekly or args.monthly or args.all or args.general):
        logger.info("No specific action requested, running daily trend clustering")
        daily_trends = get_daily_clustered_trends(method=args.method, n_clusters=args.clusters, limit=args.limit)
        print(f"Top daily clustered trends:")
        for i, trend in enumerate(daily_trends[:5]):
            keywords = ", ".join(trend['keywords'][:3])
            print(f"{i+1}. {keywords} - {trend['article_count']} articles")