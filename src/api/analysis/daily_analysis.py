# daily_analysis.py
import datetime

def fetch_today_analysis(db):
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    doc_id = f"daily_analysis_{today}"

    doc_ref = db.collection('daily_analysis').document(doc_id)
    doc = doc_ref.get()

    if doc.exists:
        return doc.to_dict()
    else:
        return None
