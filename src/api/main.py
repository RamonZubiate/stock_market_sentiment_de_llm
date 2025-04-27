from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import firebase_admin
from firebase_admin import credentials, firestore
import os
from pathlib import Path

# Set up path to service account JSON
base_dir = Path(__file__).resolve().parent
service_account_path = base_dir / "market-sentiment-de-llm-firebase-adminsdk-fbsvc-9b39350acd.json"

# Initialize Firebase App with service account
cred = credentials.Certificate(str(service_account_path))
firebase_admin.initialize_app(cred)

# Get Firestore client
db = firestore.client()

from analysis.daily_analysis import fetch_today_analysis

# Create FastAPI app
app = FastAPI()

# Allow CORS (React frontend needs it)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Route to get today's analysis
@app.get("/daily-analysis")
async def get_daily_analysis():
    analysis = fetch_today_analysis(db)
    if analysis:
        return analysis
    else:
        return {"error": "No daily analysis found for today."}
