# trend_analysis_dagster/ops/cluster_trends_ops.py
from dagster import op, Out, In
import torch
from sklearn.decomposition import PCA
import nltk
import logging
from typing import List, Dict, Any, Tuple
import numpy as np
from datetime import datetime, timedelta
from sklearn.cluster import KMeans, DBSCAN, AgglomerativeClustering
from sklearn.metrics import silhouette_score
from sklearn.neighbors import NearestNeighbors
from transformers import AutoModel
import traceback

logger = logging.getLogger(__name__)

# Download NLTK data if not already present
try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt')

def mean_pooling(model_output, attention_mask):
    """Mean pooling for BERT embeddings."""
    token_embeddings = model_output.last_hidden_state
    input_mask_expanded = attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
    sum_embeddings = torch.sum(token_embeddings * input_mask_expanded, dim=1)
    sum_mask = torch.clamp(input_mask_expanded.sum(dim=1), min=1e-9)
    return sum_embeddings / sum_mask

def prepare_article_text(article: Dict[str, Any]) -> str:
    """Prepare article text for embedding by combining title, description, and content."""
    title = article.get('title', '')
    description = article.get('description', '')
    content = article.get('content', '')
    
    # Combine title + partial content for embedding
    if len(content) > 1000:
        article_text = f"{title}. {description} {content[:1000]}"
    else:
        article_text = f"{title}. {description} {content}"
        
    return article_text

@op(
    out={"article_vectors_and_ids": Out()},
    required_resource_keys={"firestore", "openai", "finbert"}
)
def vectorize_articles(context, articles: List[Dict[str, Any]], use_finbert: bool = True) -> Tuple[np.ndarray, List[str]]:
    """Op to vectorize articles using either FinBERT or OpenAI embeddings."""
    try:
        article_vectors = []
        article_ids = []
        
        if use_finbert and context.resources.finbert["finbert_available"]:
            # Use FinBERT for vectorization with mean pooling
            base_model = AutoModel.from_pretrained("ProsusAI/finbert")
            tokenizer = context.resources.finbert["sentiment_tokenizer"]
            
            for article in articles:
                article_id = article.get('id')
                article_text = prepare_article_text(article)
                
                inputs = tokenizer(
                    article_text,
                    return_tensors="pt",
                    truncation=True,
                    max_length=512,
                    padding="max_length"
                )
                
                with torch.no_grad():
                    outputs = base_model(**inputs)
                
                pooled_embedding = mean_pooling(outputs, inputs['attention_mask'])
                vector = pooled_embedding.squeeze().cpu().numpy()
                
                # L2 normalize
                norm = np.linalg.norm(vector)
                if norm > 0:
                    vector = vector / norm
                
                article_vectors.append(vector)
                article_ids.append(article_id)
        else:
            # Use OpenAI embeddings
            for article in articles:
                article_id = article.get('id')
                article_text = prepare_article_text(article)
                
                response = context.resources.openai.Embedding.create(
                    input=article_text,
                    model="text-embedding-ada-002"
                )
                vector = np.array(response['data'][0]['embedding'])
                
                # L2 normalize
                norm = np.linalg.norm(vector)
                if norm > 0:
                    vector = vector / norm
                
                article_vectors.append(vector)
                article_ids.append(article_id)
                
        return np.array(article_vectors), article_ids
    except Exception as e:
        context.log.error(f"Error in vectorize_articles: {str(e)}")
        context.log.error(traceback.format_exc())
        raise

@op(
    ins={"article_vectors_and_ids": In()},
    out={"reduced_vectors_and_ids": Out()}
)
def reduce_dimensions(context, article_vectors_and_ids: Tuple[np.ndarray, List[str]], n_components: int = 50):
    """Op to reduce dimensions of article vectors using PCA."""
    try:
        article_vectors, article_ids = article_vectors_and_ids
        pca = PCA(n_components=n_components)
        reduced_vectors = pca.fit_transform(article_vectors)
        return reduced_vectors, article_ids
    except Exception as e:
        context.log.error(f"Error in reduce_dimensions: {str(e)}")
        context.log.error(traceback.format_exc())
        raise

@op(
    ins={"reduced_vectors_and_ids": In()},
    out={"clusters": Out()},
    config_schema={
        "method": str,
        "n_clusters": int,
        "auto_clusters": bool
    }
)
def cluster_articles(context, reduced_vectors_and_ids: Tuple[np.ndarray, List[str]]):
    """Op to cluster articles using specified method."""
    try:
        article_vectors, article_ids = reduced_vectors_and_ids
        method = context.op_config["method"]
        n_clusters = context.op_config["n_clusters"]
        auto_clusters = context.op_config.get("auto_clusters", False)
        
        if len(article_vectors) < 2:
            context.log.warning("Not enough articles to cluster")
            return {}
            
        # Auto-determine number of clusters if requested
        if auto_clusters:
            best_score = -1
            best_n = 2
            max_n = min(50, len(article_vectors) // 2)
            
            for n in range(2, max_n + 1):
                if n >= len(article_vectors):
                    break
                    
                km = KMeans(n_clusters=n, random_state=42, n_init=10)
                labels = km.fit_predict(article_vectors)
                
                if len(set(labels)) <= 1:
                    continue
                    
                score = silhouette_score(article_vectors, labels)
                context.log.info(f"Silhouette score for {n} clusters: {score:.4f}")
                if score > best_score:
                    best_score = score
                    best_n = n
            
            n_clusters = best_n
            context.log.info(f"Automatically determined optimal clusters: {n_clusters}")
        
        # Choose clustering algorithm
        if method == 'kmeans':
            clustering = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
            labels = clustering.fit_predict(article_vectors)
        elif method == 'dbscan':
            # Determine epsilon using nearest neighbors
            nn = NearestNeighbors(n_neighbors=2)
            nn.fit(article_vectors)
            distances = np.sort(nn.kneighbors(article_vectors)[0][:, 1])
            epsilon = np.percentile(distances, 90)
            clustering = DBSCAN(eps=epsilon, min_samples=2)
            labels = clustering.fit_predict(article_vectors)
        elif method == 'agglomerative':
            clustering = AgglomerativeClustering(n_clusters=n_clusters)
            labels = clustering.fit_predict(article_vectors)
        else:
            raise ValueError(f"Unsupported clustering method: {method}")
        
        # Create cluster mapping
        clusters = {}
        for i, label in enumerate(labels):
            if label not in clusters:
                clusters[label] = []
            clusters[label].append(article_ids[i])
        
        # Log cluster distribution
        for label in set(labels):
            count = len(clusters[label])
            context.log.info(f"Cluster {label}: {count} articles")
            
        return clusters
    except Exception as e:
        context.log.error(f"Error in cluster_articles: {str(e)}")
        context.log.error(traceback.format_exc())
        raise

@op(
    ins={"clusters": In(), "articles": In()},
    out={"cluster_keywords": Out()},
    required_resource_keys={"openai"}
)
def extract_cluster_keywords(context, clusters: Dict[int, List[str]], articles: List[Dict[str, Any]], top_n: int = 10):
    """Op to extract keywords for each cluster using OpenAI."""
    try:
        cluster_keywords = {}
        for cluster_id, article_ids in clusters.items():
            cluster_articles = [a for a in articles if a['id'] in article_ids]
            texts = [prepare_article_text(a) for a in cluster_articles]
            
            # Use OpenAI to extract keywords
            response = context.resources.openai.ChatCompletion.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": "Extract the most important keywords from the following text."},
                    {"role": "user", "content": "\n".join(texts)}
                ]
            )
            
            keywords = response.choices[0].message.content.split(',')
            cluster_keywords[cluster_id] = keywords[:top_n]
            
        return cluster_keywords
    except Exception as e:
        context.log.error(f"Error in extract_cluster_keywords: {str(e)}")
        context.log.error(traceback.format_exc())
        raise

@op(
    out={"daily_clusters": Out()},
    required_resource_keys={"firestore", "openai", "finbert"},
    config_schema={
        "method": str,
        "n_clusters": int,
        "auto_clusters": bool
    }
)
def get_daily_clustered_trends(context):
    """Op to get and cluster daily trends."""
    try:
        # Get config values
        method = context.op_config.get("method", "kmeans")
        n_clusters = context.op_config.get("n_clusters", 5)
        limit = 200  # Default limit
        
        # Get Firestore DB client
        db = context.resources.firestore.get_db()
        
        # Calculate date range for today
        today = datetime.now()
        today_str = today.strftime('%Y-%m-%d')
        
        context.log.info(f"Getting daily trends for {today_str}")
        
        # Try querying with proper date parsing for ISO 8601 dates (publishedAt format)
        articles_ref = db.collection('articles')
        
        # In Firestore, we can't directly filter ISO 8601 strings, so we'll fetch and filter client-side
        query = articles_ref.limit(limit)
        
        articles = []
        today_articles = []
        yesterday_articles = []

        for doc in query.stream():
            article = doc.to_dict()
            article['id'] = doc.id
            
            # Parse the publishedAt timestamp
            published_at = article.get('publishedAt')
            if published_at:
                try:
                    # Parse ISO 8601 format (2025-04-25T21:26:11Z)
                    pub_date = datetime.strptime(published_at, '%Y-%m-%dT%H:%M:%SZ')
                    
                    # Check if it's from today
                    if pub_date.date() == today.date():
                        today_articles.append(article)
                    # Or yesterday
                    elif pub_date.date() == (today - timedelta(days=1)).date():
                        yesterday_articles.append(article)
                except (ValueError, TypeError):
                    context.log.warning(f"Could not parse date: {published_at}")
                    # Add to articles anyway to ensure we have enough for clustering
                    articles.append(article)
        
        # Combine today's articles first, then yesterday's if needed
        articles = today_articles
        context.log.info(f"Found {len(articles)} articles published today")
        
        # If we didn't get enough articles from today, add some from yesterday
        if len(articles) < 20 and yesterday_articles:
            needed = min(20 - len(articles), len(yesterday_articles))
            articles.extend(yesterday_articles[:needed])
            context.log.info(f"Added {needed} articles from yesterday")
                               
        context.log.info(f"Retrieved {len(articles)} articles for daily trend clustering")
        
        if len(articles) < 5:
            context.log.warning("Not enough articles for meaningful clustering")
            return []
        
        # ---------------
        # VECTORIZATION - Implement directly instead of calling vectorize_articles op
        # ---------------
        article_vectors = []
        article_ids = []
        
        finbert_available = context.resources.finbert.get("finbert_available", False)
        if finbert_available:
            try:
                from transformers import AutoModel
                
                # Get FinBERT model and tokenizer
                tokenizer = context.resources.finbert.get("sentiment_tokenizer")
                base_model = AutoModel.from_pretrained("ProsusAI/finbert")
                
                for article in articles:
                    article_id = article.get('id')
                    
                    # Prepare article text
                    title = article.get('title', '')
                    description = article.get('description', '')
                    content = article.get('content', '')
                    
                    if len(content) > 1000:
                        article_text = f"{title}. {description} {content[:1000]}"
                    else:
                        article_text = f"{title}. {description} {content}"
                    
                    # Tokenize and get embedding
                    inputs = tokenizer(
                        article_text,
                        return_tensors="pt",
                        truncation=True,
                        max_length=512,
                        padding="max_length"
                    )
                    
                    with torch.no_grad():
                        outputs = base_model(**inputs)
                    
                    # Mean pooling
                    token_embeddings = outputs.last_hidden_state
                    input_mask_expanded = inputs['attention_mask'].unsqueeze(-1).expand(token_embeddings.size()).float()
                    sum_embeddings = torch.sum(token_embeddings * input_mask_expanded, dim=1)
                    sum_mask = torch.clamp(input_mask_expanded.sum(dim=1), min=1e-9)
                    pooled_embedding = sum_embeddings / sum_mask
                    
                    vector = pooled_embedding.squeeze().cpu().numpy()
                    
                    # L2 normalize
                    norm = np.linalg.norm(vector)
                    if norm > 0:
                        vector = vector / norm
                    
                    article_vectors.append(vector)
                    article_ids.append(article_id)
            except Exception as e:
                context.log.error(f"FinBERT vectorization failed: {e}")
                context.log.error(traceback.format_exc())
                # Will fall back to OpenAI embeddings
                article_vectors = []
                article_ids = []
        
        # If FinBERT wasn't available or failed, use OpenAI embeddings
        if not article_vectors:
            for article in articles:
                article_id = article.get('id')
                
                # Prepare article text
                title = article.get('title', '')
                description = article.get('description', '')
                content = article.get('content', '')
                
                if len(content) > 1000:
                    article_text = f"{title}. {description} {content[:1000]}"
                else:
                    article_text = f"{title}. {description} {content}"
                
                # Get embedding from OpenAI
                try:
                    response = context.resources.openai.Embedding.create(
                        input=article_text,
                        model="text-embedding-ada-002"
                    )
                    vector = np.array(response['data'][0]['embedding'])
                    
                    # L2 normalize
                    norm = np.linalg.norm(vector)
                    if norm > 0:
                        vector = vector / norm
                    
                    article_vectors.append(vector)
                    article_ids.append(article_id)
                except Exception as e:
                    context.log.error(f"OpenAI embedding failed for article {article_id}: {e}")
        
        # Check if we have any vectors
        if len(article_vectors) == 0:
            context.log.error("Failed to vectorize any articles for daily trends")
            return []
            
        article_vectors = np.array(article_vectors)
        
        # ----------------
        # DIMENSIONALITY REDUCTION - Implement directly
        # ----------------
        try:
            # Apply PCA for dimensionality reduction
            n_components = min(50, article_vectors.shape[1], len(article_vectors))
            pca = PCA(n_components=n_components)
            reduced_vectors = pca.fit_transform(article_vectors)
            context.log.info(f"Reduced dimensions from {article_vectors.shape[1]} to {reduced_vectors.shape[1]}")
        except Exception as e:
            context.log.error(f"Dimension reduction failed: {e}")
            context.log.info("Using original vectors without dimension reduction")
            reduced_vectors = article_vectors
        
        # ----------------
        # CLUSTERING - Implement directly
        # ----------------
        if len(reduced_vectors) < 2:
            context.log.warning("Not enough articles to cluster")
            return []
            
        # Auto-determine number of clusters if requested
        auto_clusters = context.op_config.get("auto_clusters", False)
        if auto_clusters:
            best_score = -1
            best_n = 2
            max_n = min(50, len(reduced_vectors) // 2)
            
            for n in range(2, max_n + 1):
                if n >= len(reduced_vectors):
                    break
                    
                km = KMeans(n_clusters=n, random_state=42, n_init=10)
                labels = km.fit_predict(reduced_vectors)
                
                if len(set(labels)) <= 1:
                    continue
                    
                score = silhouette_score(reduced_vectors, labels)
                context.log.info(f"Silhouette score for {n} clusters: {score:.4f}")
                if score > best_score:
                    best_score = score
                    best_n = n
            
            n_clusters = best_n
            context.log.info(f"Automatically determined optimal clusters: {n_clusters}")
        
        # Choose clustering algorithm
        if method == 'kmeans':
            clustering = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
            labels = clustering.fit_predict(reduced_vectors)
        elif method == 'dbscan':
            # Determine epsilon using nearest neighbors
            nn = NearestNeighbors(n_neighbors=2)
            nn.fit(reduced_vectors)
            distances = np.sort(nn.kneighbors(reduced_vectors)[0][:, 1])
            epsilon = np.percentile(distances, 90)
            clustering = DBSCAN(eps=epsilon, min_samples=2)
            labels = clustering.fit_predict(reduced_vectors)
        elif method == 'agglomerative':
            clustering = AgglomerativeClustering(n_clusters=n_clusters)
            labels = clustering.fit_predict(reduced_vectors)
        else:
            # Default to kmeans
            clustering = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
            labels = clustering.fit_predict(reduced_vectors)
        
        # Create cluster mapping
        clusters = {}
        for article_id, label in zip(article_ids, labels):
            if article_id not in clusters:
                clusters[article_id] = int(label)
        
        # Log cluster distribution
        cluster_counts = {}
        for label in labels:
            label = int(label)
            if label not in cluster_counts:
                cluster_counts[label] = 0
            cluster_counts[label] += 1
            
        for cluster_id, count in sorted(cluster_counts.items()):
            context.log.info(f"Cluster {cluster_id}: {count} articles")
        
        # ----------------
        # KEYWORD EXTRACTION - Implement directly
        # ----------------
        cluster_keywords = {}
        
        # Group articles by cluster
        cluster_articles = {}
        for article in articles:
            article_id = article.get('id')
            cluster_id = clusters.get(article_id)
            if cluster_id is None:
                continue
                
            if cluster_id not in cluster_articles:
                cluster_articles[cluster_id] = []
            cluster_articles[cluster_id].append(article)
        
        # Extract keywords for each cluster
        for cluster_id, cluster_docs in cluster_articles.items():
            # Skip small clusters
            if len(cluster_docs) < 2:
                cluster_keywords[cluster_id] = ["too_few_articles"]
                continue
                
            # Prepare texts for OpenAI
            texts = []
            for article in cluster_docs[:5]:  # Limit to 5 articles for API efficiency
                title = article.get('title', '')
                description = article.get('description', '')
                texts.append(f"{title}. {description}")
            
            # Use OpenAI to extract keywords
            try:
                response = context.resources.openai.generate_completion(
                    model="gpt-4",  # Specify the model you want to use
                    messages=[
                        {"role": "system", "content": "Extract 5-10 important keywords or phrases that represent the main topics in these articles. Return as a comma-separated list."},
                        {"role": "user", "content": "\n\n".join(texts)}  # texts is a list, joining them as a single string
                    ]
                )

                keywords = response.choices[0].message.content.split(',')
                # Clean up keywords
                keywords = [k.strip() for k in keywords]
                cluster_keywords[cluster_id] = keywords[:10]  # Limit to 10 keywords
            except Exception as e:
                context.log.error(f"Keyword extraction failed for cluster {cluster_id}: {e}")
                # Fallback - use simple word frequency
                all_text = " ".join([a.get('title', '') + " " + a.get('description', '') for a in cluster_docs])
                words = all_text.lower().split()
                word_freq = {}
                for word in words:
                    if len(word) > 3:
                        word_freq[word] = word_freq.get(word, 0) + 1
                stopwords = {"the", "and", "a", "to", "of", "in", "that", "for", "is", "on", "with"}
                keywords = [(w, f) for w, f in word_freq.items() if w not in stopwords]
                keywords.sort(key=lambda x: x[1], reverse=True)
                cluster_keywords[cluster_id] = [k[0] for k in keywords[:10]]
        
        # ----------------
        # PREPARE OUTPUT TRENDS
        # ----------------
        # Group articles by cluster
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
            'generated_at': datetime.now()
        })
        
        context.log.info(f"Stored {len(trends)} daily trends as '{daily_doc_id}'")

        context.log.info("Updating processed flag for articles...")
        update_count = 0
        
        for article in articles:
            try:
                article_id = article.get('id')
                # Get a reference to the article document
                article_ref = db.collection('articles').document(article_id)
                
                # Update only the processed field to True
                article_ref.update({'processed': True})
                update_count += 1
            except Exception as e:
                context.log.error(f"Error updating processed flag for article {article_id}: {e}")
        
        context.log.info(f"Updated processed flag for {update_count} articles")
        
        
        return trends
        
    except Exception as e:
        context.log.error(f"Error in get_daily_clustered_trends: {str(e)}")
        context.log.error(traceback.format_exc())
        return []
    
@op(
    out={"weekly_clusters": Out()},
    required_resource_keys={"firestore", "openai", "finbert"},
    config_schema={
        "method": str,
        "n_clusters": int,
        "auto_clusters": bool
    }
)
def get_weekly_clustered_trends(context):
    """Op to get and cluster weekly trends."""
    try:
        # Get config values
        method = context.op_config.get("method", "kmeans")
        n_clusters = context.op_config.get("n_clusters", 10)
        limit = 500  # Default limit for weekly trends
        
        # Get Firestore DB client
        db = context.resources.firestore.get_db()
        
        # Calculate date range for current week (Monday to today)
        today = datetime.now()
        start_of_week = today - timedelta(days=today.weekday())
        start_of_week = datetime(start_of_week.year, start_of_week.month, start_of_week.day, 0, 0, 0)
        end_of_today = datetime(today.year, today.month, today.day, 23, 59, 59)
        
        # Get week number for document ID
        week_num = today.isocalendar()[1]
        week_doc_id = f"weekly_{today.year}_W{week_num}"
        
        context.log.info(f"Getting weekly trends from {start_of_week.strftime('%Y-%m-%d')} to {end_of_today.strftime('%Y-%m-%d')}")
        
        # Try querying with proper date parsing for ISO 8601 dates (publishedAt format)
        articles_ref = db.collection('articles')
        
        # In Firestore, we can't directly filter ISO 8601 strings, so we'll fetch and filter client-side
        query = articles_ref.limit(limit)
        
        articles = []
        week_articles = []
        prev_week_articles = []

                # After clustering and processing is complete, update the processed flag
        context.log.info("Updating processed flag for articles...")
        update_count = 0

        for article_id in article_ids:
            try:
                # Get a reference to the article document
                article_ref = db.collection('articles').document(article_id)
                
                # Update only the processed field to True
                article_ref.update({'processed': True})
                update_count += 1
            except Exception as e:
                context.log.error(f"Error updating processed flag for article {article_id}: {e}")

        context.log.info(f"Updated processed flag for {update_count} articles")
        
        for doc in query.stream():
            article = doc.to_dict()
            article['id'] = doc.id

            # Parse the publishedAt timestamp
            published_at = article.get('publishedAt')
            if published_at:
                try:
                    # Parse ISO 8601 format (2025-04-25T21:26:11Z)
                    pub_date = datetime.strptime(published_at, '%Y-%m-%dT%H:%M:%SZ')
                    
                    # Check if it's from this week
                    if pub_date >= start_of_week and pub_date <= end_of_today:
                        week_articles.append(article)
                    # Or last week
                    elif pub_date >= (start_of_week - timedelta(days=7)) and pub_date < start_of_week:
                        prev_week_articles.append(article)
                except (ValueError, TypeError):
                    context.log.warning(f"Could not parse date: {published_at}")
                    # Add to articles anyway to ensure we have enough for clustering
                    articles.append(article)
        
        # Combine this week's articles first, then previous week's if needed
        articles = week_articles
        context.log.info(f"Found {len(articles)} articles published this week")
        
        # If we didn't get enough articles from this week, add some from previous week
        if len(articles) < 20 and prev_week_articles:
            needed = min(20 - len(articles), len(prev_week_articles))
            articles.extend(prev_week_articles[:needed])
            context.log.info(f"Added {needed} articles from previous week")
                               
        context.log.info(f"Retrieved {len(articles)} articles for weekly trend clustering")
        
        if len(articles) < 10:
            context.log.warning("Not enough articles for meaningful clustering")
            return []
        
        # ---------------
        # VECTORIZATION - Implement directly instead of calling vectorize_articles op
        # ---------------
        article_vectors = []
        article_ids = []
        
        finbert_available = context.resources.finbert.get("finbert_available", False)
        if finbert_available:
            try:
                from transformers import AutoModel
                
                # Get FinBERT model and tokenizer
                tokenizer = context.resources.finbert.get("sentiment_tokenizer")
                base_model = AutoModel.from_pretrained("ProsusAI/finbert")
                
                for article in articles:
                    article_id = article.get('id')
                    
                    # Prepare article text
                    title = article.get('title', '')
                    description = article.get('description', '')
                    content = article.get('content', '')
                    
                    if len(content) > 1000:
                        article_text = f"{title}. {description} {content[:1000]}"
                    else:
                        article_text = f"{title}. {description} {content}"
                    
                    # Tokenize and get embedding
                    inputs = tokenizer(
                        article_text,
                        return_tensors="pt",
                        truncation=True,
                        max_length=512,
                        padding="max_length"
                    )
                    
                    with torch.no_grad():
                        outputs = base_model(**inputs)
                    
                    # Mean pooling
                    token_embeddings = outputs.last_hidden_state
                    input_mask_expanded = inputs['attention_mask'].unsqueeze(-1).expand(token_embeddings.size()).float()
                    sum_embeddings = torch.sum(token_embeddings * input_mask_expanded, dim=1)
                    sum_mask = torch.clamp(input_mask_expanded.sum(dim=1), min=1e-9)
                    pooled_embedding = sum_embeddings / sum_mask
                    
                    vector = pooled_embedding.squeeze().cpu().numpy()
                    
                    # L2 normalize
                    norm = np.linalg.norm(vector)
                    if norm > 0:
                        vector = vector / norm
                    
                    article_vectors.append(vector)
                    article_ids.append(article_id)
            except Exception as e:
                context.log.error(f"FinBERT vectorization failed: {e}")
                context.log.error(traceback.format_exc())
                # Will fall back to OpenAI embeddings
                article_vectors = []
                article_ids = []
        
        # If FinBERT wasn't available or failed, use OpenAI embeddings
        if not article_vectors:
            for article in articles:
                article_id = article.get('id')
                
                # Prepare article text
                title = article.get('title', '')
                description = article.get('description', '')
                content = article.get('content', '')
                
                if len(content) > 1000:
                    article_text = f"{title}. {description} {content[:1000]}"
                else:
                    article_text = f"{title}. {description} {content}"
                
                # Get embedding from OpenAI
                try:
                    response = context.resources.openai.Embedding.create(
                        input=article_text,
                        model="text-embedding-ada-002"
                    )
                    vector = np.array(response['data'][0]['embedding'])
                    
                    # L2 normalize
                    norm = np.linalg.norm(vector)
                    if norm > 0:
                        vector = vector / norm
                    
                    article_vectors.append(vector)
                    article_ids.append(article_id)
                except Exception as e:
                    context.log.error(f"OpenAI embedding failed for article {article_id}: {e}")
        
        # Check if we have any vectors
        if len(article_vectors) == 0:
            context.log.error("Failed to vectorize any articles for weekly trends")
            return []
            
        article_vectors = np.array(article_vectors)
        
        # ----------------
        # DIMENSIONALITY REDUCTION - Implement directly
        # ----------------
        try:
            # Apply PCA for dimensionality reduction
            n_components = min(50, article_vectors.shape[1], len(article_vectors))
            pca = PCA(n_components=n_components)
            reduced_vectors = pca.fit_transform(article_vectors)
            context.log.info(f"Reduced dimensions from {article_vectors.shape[1]} to {reduced_vectors.shape[1]}")
        except Exception as e:
            context.log.error(f"Dimension reduction failed: {e}")
            context.log.info("Using original vectors without dimension reduction")
            reduced_vectors = article_vectors
        
        # ----------------
        # CLUSTERING - Implement directly
        # ----------------
        if len(reduced_vectors) < 2:
            context.log.warning("Not enough articles to cluster")
            return []
            
        # Auto-determine number of clusters if requested
        auto_clusters = context.op_config.get("auto_clusters", False)
        if auto_clusters:
            best_score = -1
            best_n = 2
            max_n = min(50, len(reduced_vectors) // 2)
            
            for n in range(2, max_n + 1):
                if n >= len(reduced_vectors):
                    break
                    
                km = KMeans(n_clusters=n, random_state=42, n_init=10)
                labels = km.fit_predict(reduced_vectors)
                
                if len(set(labels)) <= 1:
                    continue
                    
                score = silhouette_score(reduced_vectors, labels)
                context.log.info(f"Silhouette score for {n} clusters: {score:.4f}")
                if score > best_score:
                    best_score = score
                    best_n = n
            
            n_clusters = best_n
            context.log.info(f"Automatically determined optimal clusters: {n_clusters}")
        
        # Choose clustering algorithm
        if method == 'kmeans':
            clustering = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
            labels = clustering.fit_predict(reduced_vectors)
        elif method == 'dbscan':
            # Determine epsilon using nearest neighbors
            nn = NearestNeighbors(n_neighbors=2)
            nn.fit(reduced_vectors)
            distances = np.sort(nn.kneighbors(reduced_vectors)[0][:, 1])
            epsilon = np.percentile(distances, 90)
            clustering = DBSCAN(eps=epsilon, min_samples=2)
            labels = clustering.fit_predict(reduced_vectors)
        elif method == 'agglomerative':
            clustering = AgglomerativeClustering(n_clusters=n_clusters)
            labels = clustering.fit_predict(reduced_vectors)
        else:
            # Default to kmeans
            clustering = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
            labels = clustering.fit_predict(reduced_vectors)
        
        # Create cluster mapping
        clusters = {}
        for article_id, label in zip(article_ids, labels):
            if article_id not in clusters:
                clusters[article_id] = int(label)
        
        # Log cluster distribution
        cluster_counts = {}
        for label in labels:
            label = int(label)
            if label not in cluster_counts:
                cluster_counts[label] = 0
            cluster_counts[label] += 1
            
        for cluster_id, count in sorted(cluster_counts.items()):
            context.log.info(f"Cluster {cluster_id}: {count} articles")
        
        # ----------------
        # KEYWORD EXTRACTION - Implement directly
        # ----------------
        cluster_keywords = {}
        
        # Group articles by cluster
        cluster_articles = {}
        for article in articles:
            article_id = article.get('id')
            cluster_id = clusters.get(article_id)
            if cluster_id is None:
                continue
                
            if cluster_id not in cluster_articles:
                cluster_articles[cluster_id] = []
            cluster_articles[cluster_id].append(article)
        
        # Extract keywords for each cluster
        for cluster_id, cluster_docs in cluster_articles.items():
            # Skip small clusters
            if len(cluster_docs) < 2:
                cluster_keywords[cluster_id] = ["too_few_articles"]
                continue
                
            # Prepare texts for OpenAI
            texts = []
            for article in cluster_docs[:5]:  # Limit to 5 articles for API efficiency
                title = article.get('title', '')
                description = article.get('description', '')
                texts.append(f"{title}. {description}")
            
            # Use OpenAI to extract keywords
            try:
                response = context.resources.openai.generate_completion(
                    model="gpt-4",
                    messages=[
                        {"role": "system", "content": "Extract 5-10 important keywords or phrases that represent the main topics in these articles. Return as a comma-separated list."},
                        {"role": "user", "content": "\n\n".join(texts)}
                    ]
                )

                keywords = response.choices[0].message.content.split(',')
                # Clean up keywords
                keywords = [k.strip() for k in keywords]
                cluster_keywords[cluster_id] = keywords[:10]  # Limit to 10 keywords
            except Exception as e:
                context.log.error(f"Keyword extraction failed for cluster {cluster_id}: {e}")
                # Fallback - use simple word frequency
                all_text = " ".join([a.get('title', '') + " " + a.get('description', '') for a in cluster_docs])
                words = all_text.lower().split()
                word_freq = {}
                for word in words:
                    if len(word) > 3:
                        word_freq[word] = word_freq.get(word, 0) + 1
                stopwords = {"the", "and", "a", "to", "of", "in", "that", "for", "is", "on", "with"}
                keywords = [(w, f) for w, f in word_freq.items() if w not in stopwords]
                keywords.sort(key=lambda x: x[1], reverse=True)
                cluster_keywords[cluster_id] = [k[0] for k in keywords[:10]]
        
        # ----------------
        # PREPARE OUTPUT TRENDS - Weekly specific logic
        # ----------------
        # Group articles by cluster
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
            pub_date_str = article.get('publishedAt')
            if pub_date_str:
                try:
                    pub_date = datetime.strptime(pub_date_str, '%Y-%m-%dT%H:%M:%SZ')
                    day_key = pub_date.strftime('%Y-%m-%d')
                    if day_key not in cluster_data[cluster_id]['date_counts']:
                        cluster_data[cluster_id]['date_counts'][day_key] = 0
                    cluster_data[cluster_id]['date_counts'][day_key] += 1
                except (ValueError, TypeError):
                    context.log.warning(f"Could not parse date for day distribution: {pub_date_str}")
        
        # Prepare trend data
        trends = []
        for cluster_id, data in cluster_data.items():  
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
            'generated_at': datetime.now()
        })
        
        context.log.info(f"Stored {len(trends)} weekly trends as '{week_doc_id}'")
        
        context.log.info("Updating processed flag for articles...")
        update_count = 0
        
        for article in articles:
            try:
                article_id = article.get('id')
                # Get a reference to the article document
                article_ref = db.collection('articles').document(article_id)
                
                # Update only the processed field to True
                article_ref.update({'processed': True})
                update_count += 1
            except Exception as e:
                context.log.error(f"Error updating processed flag for article {article_id}: {e}")

        return trends
        
    except Exception as e:
        context.log.error(f"Error in get_weekly_clustered_trends: {str(e)}")
        context.log.error(traceback.format_exc())
        return []
    
@op(
    out={"monthly_clusters": Out()},
    required_resource_keys={"firestore", "openai", "finbert"},
    config_schema={
        "method": str,
        "n_clusters": int,
        "auto_clusters": bool
    }
)
def get_monthly_clustered_trends(context):
    """Op to get and cluster monthly trends."""
    try:
        # Get config values
        method = context.op_config.get("method", "kmeans")
        n_clusters = context.op_config.get("n_clusters", 25)
        limit = 1000  # Default limit for monthly trends
        
        # Get Firestore DB client
        db = context.resources.firestore.get_db()
        
        # Calculate date range for current month
        today = datetime.now()
        start_of_month = datetime(today.year, today.month, 1, 0, 0, 0)
        end_of_today = datetime(today.year, today.month, today.day, 23, 59, 59)
        
        # Document ID format: monthly_{YYYY}_{MM}
        month_doc_id = f"monthly_{today.year}_{today.month:02d}"
        
        context.log.info(f"Getting monthly trends from {start_of_month.strftime('%Y-%m-%d')} to {end_of_today.strftime('%Y-%m-%d')}")
        
        # Try querying with proper date parsing for ISO 8601 dates (publishedAt format)
        articles_ref = db.collection('articles')
        
        # In Firestore, we can't directly filter ISO 8601 strings, so we'll fetch and filter client-side
        query = articles_ref.limit(limit)
        
        articles = []
        month_articles = []
        prev_month_articles = []

        # After clustering and processing is complete, update the processed flag
        context.log.info("Updating processed flag for articles...")
        update_count = 0

        for article_id in article_ids:
            try:
                # Get a reference to the article document
                article_ref = db.collection('articles').document(article_id)
                
                # Update only the processed field to True
                article_ref.update({'processed': True})
                update_count += 1
            except Exception as e:
                context.log.error(f"Error updating processed flag for article {article_id}: {e}")

        context.log.info(f"Updated processed flag for {update_count} articles")
        
        for doc in query.stream():
            article = doc.to_dict()
            article['id'] = doc.id

            # Parse the publishedAt timestamp
            published_at = article.get('publishedAt')
            if published_at:
                try:
                    # Parse ISO 8601 format (2025-04-25T21:26:11Z)
                    pub_date = datetime.strptime(published_at, '%Y-%m-%dT%H:%M:%SZ')
                    
                    # Check if it's from this month
                    if pub_date >= start_of_month and pub_date <= end_of_today:
                        month_articles.append(article)
                    # Or last month
                    elif pub_date >= (start_of_month.replace(day=1) - timedelta(days=1)).replace(day=1) and pub_date < start_of_month:
                        prev_month_articles.append(article)
                except (ValueError, TypeError):
                    context.log.warning(f"Could not parse date: {published_at}")
                    # Add to articles anyway to ensure we have enough for clustering
                    articles.append(article)
        
        # Combine this month's articles first, then previous month's if needed
        articles = month_articles
        context.log.info(f"Found {len(articles)} articles published this month")
        
        # If we didn't get enough articles from this month, add some from previous month
        if len(articles) < 30 and prev_month_articles:
            needed = min(30 - len(articles), len(prev_month_articles))
            articles.extend(prev_month_articles[:needed])
            context.log.info(f"Added {needed} articles from previous month")
                               
        context.log.info(f"Retrieved {len(articles)} articles for monthly trend clustering")
        
        if len(articles) < 15:
            context.log.warning("Not enough articles for meaningful monthly clustering")
            return []
        
        # ---------------
        # VECTORIZATION - Implement directly instead of calling vectorize_articles op
        # ---------------
        article_vectors = []
        article_ids = []
        
        finbert_available = context.resources.finbert.get("finbert_available", False)
        if finbert_available:
            try:
                from transformers import AutoModel
                import torch
                
                # Get FinBERT model and tokenizer
                tokenizer = context.resources.finbert.get("sentiment_tokenizer")
                base_model = AutoModel.from_pretrained("ProsusAI/finbert")
                
                for article in articles:
                    article_id = article.get('id')
                    
                    # Prepare article text
                    title = article.get('title', '')
                    description = article.get('description', '')
                    content = article.get('content', '')
                    
                    if len(content) > 1000:
                        article_text = f"{title}. {description} {content[:1000]}"
                    else:
                        article_text = f"{title}. {description} {content}"
                    
                    # Tokenize and get embedding
                    inputs = tokenizer(
                        article_text,
                        return_tensors="pt",
                        truncation=True,
                        max_length=512,
                        padding="max_length"
                    )
                    
                    with torch.no_grad():
                        outputs = base_model(**inputs)
                    
                    # Mean pooling
                    token_embeddings = outputs.last_hidden_state
                    input_mask_expanded = inputs['attention_mask'].unsqueeze(-1).expand(token_embeddings.size()).float()
                    sum_embeddings = torch.sum(token_embeddings * input_mask_expanded, dim=1)
                    sum_mask = torch.clamp(input_mask_expanded.sum(dim=1), min=1e-9)
                    pooled_embedding = sum_embeddings / sum_mask
                    
                    vector = pooled_embedding.squeeze().cpu().numpy()
                    
                    # L2 normalize
                    norm = np.linalg.norm(vector)
                    if norm > 0:
                        vector = vector / norm
                    
                    article_vectors.append(vector)
                    article_ids.append(article_id)
            except Exception as e:
                context.log.error(f"FinBERT vectorization failed: {e}")
                context.log.error(traceback.format_exc())
                # Will fall back to OpenAI embeddings
                article_vectors = []
                article_ids = []
        
        # If FinBERT wasn't available or failed, use OpenAI embeddings
        if not article_vectors:
            for article in articles:
                article_id = article.get('id')
                
                # Prepare article text
                title = article.get('title', '')
                description = article.get('description', '')
                content = article.get('content', '')
                
                if len(content) > 1000:
                    article_text = f"{title}. {description} {content[:1000]}"
                else:
                    article_text = f"{title}. {description} {content}"
                
                # Get embedding from OpenAI
                try:
                    response = context.resources.openai.Embedding.create(
                        input=article_text,
                        model="text-embedding-ada-002"
                    )
                    vector = np.array(response['data'][0]['embedding'])
                    
                    # L2 normalize
                    norm = np.linalg.norm(vector)
                    if norm > 0:
                        vector = vector / norm
                    
                    article_vectors.append(vector)
                    article_ids.append(article_id)
                except Exception as e:
                    context.log.error(f"OpenAI embedding failed for article {article_id}: {e}")
        
        # Check if we have any vectors
        if len(article_vectors) == 0:
            context.log.error("Failed to vectorize any articles for monthly trends")
            return []
            
        article_vectors = np.array(article_vectors)
        
        # ----------------
        # DIMENSIONALITY REDUCTION - Implement directly
        # ----------------
        try:
            # Apply PCA for dimensionality reduction
            n_components = min(50, article_vectors.shape[1], len(article_vectors))
            pca = PCA(n_components=n_components)
            reduced_vectors = pca.fit_transform(article_vectors)
            context.log.info(f"Reduced dimensions from {article_vectors.shape[1]} to {reduced_vectors.shape[1]}")
        except Exception as e:
            context.log.error(f"Dimension reduction failed: {e}")
            context.log.info("Using original vectors without dimension reduction")
            reduced_vectors = article_vectors
        
        # ----------------
        # CLUSTERING - Implement directly
        # ----------------
        if len(reduced_vectors) < 2:
            context.log.warning("Not enough articles to cluster")
            return []
            
        # Auto-determine number of clusters if requested
        auto_clusters = context.op_config.get("auto_clusters", False)
        if auto_clusters:
            best_score = -1
            best_n = 2
            max_n = min(50, len(reduced_vectors) // 2)
            
            for n in range(2, max_n + 1):
                if n >= len(reduced_vectors):
                    break
                    
                km = KMeans(n_clusters=n, random_state=42, n_init=10)
                labels = km.fit_predict(reduced_vectors)
                
                if len(set(labels)) <= 1:
                    continue
                    
                score = silhouette_score(reduced_vectors, labels)
                context.log.info(f"Silhouette score for {n} clusters: {score:.4f}")
                if score > best_score:
                    best_score = score
                    best_n = n
            
            n_clusters = best_n
            context.log.info(f"Automatically determined optimal clusters: {n_clusters}")
        
        # Choose clustering algorithm
        if method == 'kmeans':
            clustering = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
            labels = clustering.fit_predict(reduced_vectors)
        elif method == 'dbscan':
            # Determine epsilon using nearest neighbors
            nn = NearestNeighbors(n_neighbors=2)
            nn.fit(reduced_vectors)
            distances = np.sort(nn.kneighbors(reduced_vectors)[0][:, 1])
            epsilon = np.percentile(distances, 90)
            clustering = DBSCAN(eps=epsilon, min_samples=2)
            labels = clustering.fit_predict(reduced_vectors)
        elif method == 'agglomerative':
            clustering = AgglomerativeClustering(n_clusters=n_clusters)
            labels = clustering.fit_predict(reduced_vectors)
        else:
            # Default to kmeans
            clustering = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
            labels = clustering.fit_predict(reduced_vectors)
        
        # Create cluster mapping
        clusters = {}
        for article_id, label in zip(article_ids, labels):
            if article_id not in clusters:
                clusters[article_id] = int(label)
        
        # Log cluster distribution
        cluster_counts = {}
        for label in labels:
            label = int(label)
            if label not in cluster_counts:
                cluster_counts[label] = 0
            cluster_counts[label] += 1
            
        for cluster_id, count in sorted(cluster_counts.items()):
            context.log.info(f"Cluster {cluster_id}: {count} articles")
        
        # ----------------
        # KEYWORD EXTRACTION - Implement directly
        # ----------------
        cluster_keywords = {}
        
        # Group articles by cluster
        cluster_articles = {}
        for article in articles:
            article_id = article.get('id')
            cluster_id = clusters.get(article_id)
            if cluster_id is None:
                continue
                
            if cluster_id not in cluster_articles:
                cluster_articles[cluster_id] = []
            cluster_articles[cluster_id].append(article)
        
        # Extract keywords for each cluster
        for cluster_id, cluster_docs in cluster_articles.items():
            # Skip small clusters
            if len(cluster_docs) < 2:
                cluster_keywords[cluster_id] = ["too_few_articles"]
                continue
                
            # Prepare texts for OpenAI
            texts = []
            for article in cluster_docs[:5]:  # Limit to 5 articles for API efficiency
                title = article.get('title', '')
                description = article.get('description', '')
                texts.append(f"{title}. {description}")
            
            # Use OpenAI to extract keywords
            try:
                response = context.resources.openai.generate_completion(
                    model="gpt-4",
                    messages=[
                        {"role": "system", "content": "Extract 5-10 important keywords or phrases that represent the main topics in these articles. Return as a comma-separated list."},
                        {"role": "user", "content": "\n\n".join(texts)}
                    ]
                )

                keywords = response.choices[0].message.content.split(',')
                # Clean up keywords
                keywords = [k.strip() for k in keywords]
                cluster_keywords[cluster_id] = keywords[:10]  # Limit to 10 keywords
            except Exception as e:
                context.log.error(f"Keyword extraction failed for cluster {cluster_id}: {e}")
                # Fallback - use simple word frequency
                all_text = " ".join([a.get('title', '') + " " + a.get('description', '') for a in cluster_docs])
                words = all_text.lower().split()
                word_freq = {}
                for word in words:
                    if len(word) > 3:
                        word_freq[word] = word_freq.get(word, 0) + 1
                stopwords = {"the", "and", "a", "to", "of", "in", "that", "for", "is", "on", "with"}
                keywords = [(w, f) for w, f in word_freq.items() if w not in stopwords]
                keywords.sort(key=lambda x: x[1], reverse=True)
                cluster_keywords[cluster_id] = [k[0] for k in keywords[:10]]
        
        # ----------------
        # PREPARE OUTPUT TRENDS - Monthly specific logic
        # ----------------
        # Group articles by cluster
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
            pub_date_str = article.get('publishedAt')
            if pub_date_str:
                try:
                    pub_date = datetime.strptime(pub_date_str, '%Y-%m-%dT%H:%M:%SZ')
                    
                    # Day distribution
                    day_key = pub_date.strftime('%Y-%m-%d')
                    if day_key not in cluster_data[cluster_id]['date_counts']:
                        cluster_data[cluster_id]['date_counts'][day_key] = 0
                    cluster_data[cluster_id]['date_counts'][day_key] += 1
                    
                    # Week distribution
                    week_num = pub_date.isocalendar()[1]
                    week_key = f"{pub_date.year}-W{week_num}"
                    if week_key not in cluster_data[cluster_id]['week_counts']:
                        cluster_data[cluster_id]['week_counts'][week_key] = 0
                    cluster_data[cluster_id]['week_counts'][week_key] += 1
                    
                except (ValueError, TypeError):
                    context.log.warning(f"Could not parse date for distribution: {pub_date_str}")
        
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
            'generated_at': datetime.now()
        })
        
        context.log.info(f"Stored {len(trends)} monthly trends as '{month_doc_id}'")
        
        context.log.info("Updating processed flag for articles...")
        update_count = 0
        
        for article in articles:
            try:
                article_id = article.get('id')
                # Get a reference to the article document
                article_ref = db.collection('articles').document(article_id)
                
                # Update only the processed field to True
                article_ref.update({'processed': True})
                update_count += 1
            except Exception as e:
                context.log.error(f"Error updating processed flag for article {article_id}: {e}")

        return trends
        
    except Exception as e:
        context.log.error(f"Error in get_monthly_clustered_trends: {str(e)}")
        context.log.error(traceback.format_exc())
        return []