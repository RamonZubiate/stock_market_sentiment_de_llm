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
def get_daily_clustered_trends(context, method: str = 'kmeans', n_clusters: int = 5, limit: int = 200):
    """Op to get and cluster daily trends."""
    try:
        db = context.resources.firestore
        today = datetime.now().strftime("%Y-%m-%d")
        
        # Get articles from Firestore
        articles_ref = db.collection('articles').where('date', '==', today).limit(limit)
        articles = [doc.to_dict() for doc in articles_ref.stream()]
        
        if not articles:
            context.log.warning(f"No articles found for date {today}")
            return None
            
        # Vectorize articles
        article_vectors, article_ids = vectorize_articles(articles)
        
        # Reduce dimensions
        reduced_vectors, _ = reduce_dimensions((article_vectors, article_ids))
        
        # Cluster articles
        clusters = cluster_articles((reduced_vectors, article_ids))
        
        # Extract keywords
        keywords = extract_cluster_keywords(clusters, articles)
        
        return {
            "date": today,
            "clusters": clusters,
            "keywords": keywords
        }
    except Exception as e:
        context.log.error(f"Error in get_daily_clustered_trends: {str(e)}")
        raise

@op(
    out={"weekly_clusters": Out()},
    required_resource_keys={"firestore", "openai", "finbert"},
    config_schema={
        "method": str,
        "n_clusters": int,
        "auto_clusters": bool
    }
)
def get_weekly_clustered_trends(context, method: str = 'kmeans', n_clusters: int = 10, limit: int = 500):
    """Op to get and cluster weekly trends."""
    try:
        db = context.resources.firestore
        today = datetime.now()
        start_of_week = today - timedelta(days=today.weekday())
        start_of_week = start_of_week.replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Get articles from the current week
        articles_ref = db.collection('articles')
        query = articles_ref.where("published_at", ">=", start_of_week).where("published_at", "<=", today).limit(limit)
        articles = [doc.to_dict() for doc in query.stream()]
        
        if not articles:
            context.log.warning(f"No articles found for week starting {start_of_week}")
            return None
            
        # Vectorize articles
        article_vectors, article_ids = vectorize_articles(articles)
        
        # Reduce dimensions
        reduced_vectors, _ = reduce_dimensions((article_vectors, article_ids))
        
        # Cluster articles
        clusters = cluster_articles((reduced_vectors, article_ids))
        
        # Extract keywords and summaries
        keywords = extract_cluster_keywords(clusters, articles)
        
        # Calculate trend direction for each cluster
        trend_directions = {}
        for cluster_id, article_ids in clusters.items():
            cluster_articles = [a for a in articles if a['id'] in article_ids]
            dates = [a.get('published_at') for a in cluster_articles if a.get('published_at')]
            if len(dates) >= 2:
                first_half = [d for d in dates if d <= start_of_week + timedelta(days=3.5)]
                second_half = [d for d in dates if d > start_of_week + timedelta(days=3.5)]
                if len(first_half) > 0 and len(second_half) > 0:
                    trend_directions[cluster_id] = "increasing" if len(second_half) > len(first_half) else "decreasing"
                else:
                    trend_directions[cluster_id] = "stable"
            else:
                trend_directions[cluster_id] = "stable"
        
        return {
            "start_date": start_of_week.strftime("%Y-%m-%d"),
            "end_date": today.strftime("%Y-%m-%d"),
            "clusters": clusters,
            "keywords": keywords,
            "trend_directions": trend_directions
        }
    except Exception as e:
        context.log.error(f"Error in get_weekly_clustered_trends: {str(e)}")
        raise

@op(
    out={"monthly_clusters": Out()},
    required_resource_keys={"firestore", "openai", "finbert"},
    config_schema={
        "method": str,
        "n_clusters": int,
        "auto_clusters": bool
    }
)
def get_monthly_clustered_trends(context, method: str = 'kmeans', n_clusters: int = 15, limit: int = 1000):
    """Op to get and cluster monthly trends."""
    try:
        db = context.resources.firestore
        today = datetime.now()
        start_of_month = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        
        # Get articles from the current month
        articles_ref = db.collection('articles')
        query = articles_ref.where("published_at", ">=", start_of_month).where("published_at", "<=", today).limit(limit)
        articles = [doc.to_dict() for doc in query.stream()]
        
        if not articles:
            context.log.warning(f"No articles found for month starting {start_of_month}")
            return None
            
        # Vectorize articles
        article_vectors, article_ids = vectorize_articles(articles)
        
        # Reduce dimensions
        reduced_vectors, _ = reduce_dimensions((article_vectors, article_ids))
        
        # Cluster articles
        clusters = cluster_articles((reduced_vectors, article_ids))
        
        # Extract keywords and summaries
        keywords = extract_cluster_keywords(clusters, articles)
        
        # Calculate trend strength and direction for each cluster
        trend_metrics = {}
        for cluster_id, article_ids in clusters.items():
            cluster_articles = [a for a in articles if a['id'] in article_ids]
            dates = [a.get('published_at') for a in cluster_articles if a.get('published_at')]
            
            if len(dates) >= 3:
                # Split month into thirds
                first_third = [d for d in dates if d <= start_of_month + timedelta(days=10)]
                second_third = [d for d in dates if start_of_month + timedelta(days=10) < d <= start_of_month + timedelta(days=20)]
                last_third = [d for d in dates if d > start_of_month + timedelta(days=20)]
                
                # Calculate trend direction
                if len(last_third) > len(first_third):
                    direction = "increasing"
                elif len(last_third) < len(first_third):
                    direction = "decreasing"
                else:
                    direction = "stable"
                
                # Calculate trend strength
                total_articles = len(dates)
                strength = total_articles / len(articles)  # Proportion of total articles
                
                trend_metrics[cluster_id] = {
                    "direction": direction,
                    "strength": strength,
                    "total_articles": total_articles,
                    "distribution": {
                        "first_third": len(first_third),
                        "second_third": len(second_third),
                        "last_third": len(last_third)
                    }
                }
            else:
                trend_metrics[cluster_id] = {
                    "direction": "stable",
                    "strength": 0.0,
                    "total_articles": len(dates),
                    "distribution": {"first_third": 0, "second_third": 0, "last_third": 0}
                }
        
        return {
            "start_date": start_of_month.strftime("%Y-%m-%d"),
            "end_date": today.strftime("%Y-%m-%d"),
            "clusters": clusters,
            "keywords": keywords,
            "trend_metrics": trend_metrics
        }
    except Exception as e:
        context.log.error(f"Error in get_monthly_clustered_trends: {str(e)}")
        raise
