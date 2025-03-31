import os
import logging
import argparse
import traceback
from concurrent.futures import ThreadPoolExecutor
from google.cloud import firestore

# Configure logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
import torch
from transformers import BartForConditionalGeneration, BartTokenizer, BertTokenizer, BertForSequenceClassification

# Initialize Firebase (if not already initialized)
import firebase_admin
from firebase_admin import credentials, firestore
if not firebase_admin._apps:
    try:
        cred = credentials.Certificate("server/market-sentiment-de-llm-firebase-adminsdk-fbsvc-9b39350acd.json")
        firebase_admin.initialize_app(cred)
        db = firestore.client()
        logger.info("Firebase initialized successfully")
    except Exception as e:
        logger.error(f"Firebase initialization error: {e}")
        raise


# Initialize FinBERT model and tokenizer
sentiment_model = BertForSequenceClassification.from_pretrained('ProsusAI/finbert')
sentiment_tokenizer = BertTokenizer.from_pretrained('ProsusAI/finbert')
FINBERT_AVAILABLE = True

# Initialize BART model and tokenizer for summarization
summarizer = BartForConditionalGeneration.from_pretrained('facebook/bart-large-cnn')
SUMMARIZER_AVAILABLE = True

def analyze_sentiment(text):
    """Analyze sentiment using FinBERT or fallback to a simplistic approach."""
    global FINBERT_AVAILABLE
    if FINBERT_AVAILABLE:
        try:
            max_length = 512
            inputs = sentiment_tokenizer(text, return_tensors="pt", truncation=True, max_length=max_length)
            with torch.no_grad():
                outputs = sentiment_model(**inputs)
                scores = torch.nn.functional.softmax(outputs.logits, dim=1)
                label_id = scores.argmax().item()
                sentiment_label = sentiment_model.config.id2label[label_id]
                sentiment_score = scores.max().item()
            return {
                "label": sentiment_label.lower(),  # "positive", "negative", or "neutral"
                "score": sentiment_score
            }
        except Exception as e:
            logger.warning(f"FinBERT sentiment analysis failed: {e}. Fallback used.")
            FINBERT_AVAILABLE = False

    # Basic fallback
    logger.info("Using basic sentiment analysis fallback")
    positive_words = ['increase','gain','up','rise','improve','positive','profit','success','bull','bullish']
    negative_words = ['decrease','loss','down','fall','decline','negative','concern','risk','bear','bearish']

    text_lower = text.lower()
    positive_count = sum(1 for w in positive_words if w in text_lower)
    negative_count = sum(1 for w in negative_words if w in text_lower)

    if positive_count > negative_count:
        return {"label": "positive", "score": 0.7}
    elif negative_count > positive_count:
        return {"label": "negative", "score": 0.7}
    else:
        return {"label": "neutral", "score": 0.8}


###############################################################################
# 2) SUMMARIZATION
###############################################################################
def generate_summary(text, min_length=100, max_length=300):
    """Generate a summary using BART or fallback to basic extractive approach."""
    global SUMMARIZER_AVAILABLE
    if SUMMARIZER_AVAILABLE:
        try:
            # BART can only handle limited text length
            if len(text) > 1024:
                text = text[:1024]
            summary = summarizer(text, max_length=max_length, min_length=min_length)[0]['summary_text']
            return summary
        except Exception as e:
            logger.warning(f"BART summarization failed: {e}. Using fallback.")
            SUMMARIZER_AVAILABLE = False
    
def analyze_article(article_id):
    """Analyze an article and store results in Firestore."""
    try:
        # Fetch article
        article_ref = db.collection('articles').document(article_id)
        article_doc = article_ref.get()
        if not article_doc.exists:
            logger.warning(f"Article {article_id} not found")
            return None

        article_data = article_doc.to_dict()
        title = article_data.get('title', '')
        content = article_data.get('content', '')
        description = article_data.get('description', '')
        category = article_data.get('category', '')

        logger.info(f"Analyzing article: {title}")

        # Combine text
        full_text = f"{title}. {description} {content}"

        # 1) Sentiment
        sentiment_info = analyze_sentiment(full_text)
        logger.info(f"Sentiment: {sentiment_info['label']} (score={sentiment_info['score']:.2f})")

        # 2) Summarization
        summary = generate_summary(full_text)
        logger.info(f"Generated summary: {len(summary)} chars")

        # Prepare to store
        analysis_results = {
            "article_id": article_id,
            "sentiment": sentiment_info['label'],
            "sentiment_score": sentiment_info['score'],
            "summary": summary,
            "category": category,
            "analysis_date": firestore.SERVER_TIMESTAMP
        }

        # Store analysis
        analysis_ref = db.collection('article_analysis').document(article_id)
        analysis_ref.set(analysis_results)
        logger.info(f"Stored analysis for article {article_id}")

        # Mark the article as processed
        article_ref.update({
            "processed": True,
            "processing_date": firestore.SERVER_TIMESTAMP
        })

        logger.info(f"Analysis complete for article {article_id}")
        return analysis_results


    except Exception as e:
        logger.error(f"Error analyzing article {article_id}: {e}")
        logger.error(traceback.format_exc())
        # Mark as failed
        try:
            article_ref = db.collection('articles').document(article_id)
            article_ref.update({
                "processing_error": str(e),
                "processing_failed": True
            })
        except Exception as update_error:
            logger.error(f"Could not update article with error: {update_error}")
        return None

###############################################################################
# 5) BATCH PROCESSING + TRENDS (UNCHANGED EXCEPT WE USE THE NEW analyze_article)
###############################################################################
def process_unanalyzed_articles_parallel(max_workers=4, limit=100):
    articles_ref = db.collection('articles')
    query = articles_ref.where('processed', '==', False).limit(limit)
    article_ids = [doc.id for doc in query.stream()]
    total_articles = len(article_ids)

    if total_articles == 0:
        logger.info("No unanalyzed articles found")
        return []

    logger.info(f"Found {total_articles} unanalyzed articles. Processing with {max_workers} workers.")
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_article = {executor.submit(analyze_article, aid): aid for aid in article_ids}
        for future in future_to_article:
            aid = future_to_article[future]
            try:
                res = future.result()
                if res:
                    results.append(res)
            except Exception as e:
                logger.error(f"Error processing article {aid}: {e}")

    logger.info(f"Completed analysis of {len(results)}/{total_articles} articles")
    return results

def process_single_article(article_id):
    logger.info(f"Processing single article: {article_id}")
    result = analyze_article(article_id)
    if result:
        logger.info(f"Successfully processed article {article_id}")
    else:
        logger.error(f"Failed to process article {article_id}")
    return result

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--single', type=str, help='Process a single article by ID')
    parser.add_argument('--batch', action='store_true', help='Process a batch of unanalyzed articles')
    parser.add_argument('--workers', type=int, default=4, help='Number of worker threads for batch processing')
    parser.add_argument('--limit', type=int, default=100, help='Max number of articles to process')
    args = parser.parse_args()

