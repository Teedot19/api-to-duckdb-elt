import os
from dotenv import load_dotenv
from pathlib import Path


# Load environment variables from .env
load_dotenv()

# API configuration
API_BASE_URL = "https://api.thenewsapi.com/"
TOP_NEWS_ENDPOINT = "v1/news/top"

DB_PATH = Path("news_api_data.db")

API_TOKEN = os.getenv("THENEWSAPI_TOKEN")

if not API_TOKEN:
    raise RuntimeError("THENEWSAPI_TOKEN is not set")

GSHEET_KEY=os.getenv('GOOGLE_SHEETS_KEY_FILE')

GSHEET_FILE = "1JTEBmqOG7H3eaKqKA3y8WSL2FPBW6Rm2av4IJcfPpDw"


# Default query parameters (non-secret)
DEFAULT_PARAMS = {
    'api_token': API_TOKEN,
    "published_after": "2026-01-01T00:00:00",
    "page": '1',
    "search": "Nigeria | Nigerians| Africa| Africans",
    "exclude_categories": "sports",
}






            