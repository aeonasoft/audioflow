import os
from dotenv import load_dotenv


if os.getenv("AIRFLOW__CORE__EXECUTOR", "").startswith("CeleryExecutor"):
    BASE_DIR = "/opt/airflow"
else:
    BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

env_path = os.path.join(BASE_DIR, ".env")
load_dotenv(env_path)

print(f"Config BASE_DIR: {BASE_DIR}")

ENVIRONMENT = os.getenv("APP_ENV", "mock").lower()
BBC_RSS_URL = os.getenv("BBC_RSS_URL", "https://feeds.bbci.co.uk/news/world/rss.xml")
HACKERNEWS_TOP_STORIES_URL = os.getenv("HACKERNEWS_TOP_STORIES_URL", "https://hacker-news.firebaseio.com/v0/topstories.json")
HACKERNEWS_ITEM_URL = os.getenv("HACKERNEWS_ITEM_URL", "https://hacker-news.firebaseio.com/v0/item/{}.json")
BIGQUERY_PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID")
GENAI_API_KEY = os.getenv("GENAI_API_KEY")
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", 587))
SMTP_USERNAME = os.getenv("SMTP_USERNAME")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
SMTP_SENDER_EMAIL = os.getenv("SMTP_SENDER_EMAIL")
BIGQUERY_CREDENTIALS_PATH = os.getenv("BIGQUERY_CREDENTIALS_PATH")

if BIGQUERY_CREDENTIALS_PATH and BIGQUERY_CREDENTIALS_PATH.startswith("~"):
    BIGQUERY_CREDENTIALS_PATH = os.path.expanduser(BIGQUERY_CREDENTIALS_PATH)
if BIGQUERY_CREDENTIALS_PATH and os.path.exists(BIGQUERY_CREDENTIALS_PATH):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = BIGQUERY_CREDENTIALS_PATH
    print(f"SUCCESS: Using BigQuery credentials from: {BIGQUERY_CREDENTIALS_PATH}")
else:
    raise Exception(
        f"ERROR: BIGQUERY_CREDENTIALS_PATH is invalid or does not exist: {BIGQUERY_CREDENTIALS_PATH}"
    )

DATABASES = {
    "mock": os.path.join(BASE_DIR, "data/mock/data_mock.db"),
    "dev": os.path.join(BASE_DIR, "data/dev/data_dev.db"),
    "prod": os.path.join(BASE_DIR, "data/prod/data_prod.db"),
}

CSV_DATA_FOLDER = os.path.join(BASE_DIR, f"data/{ENVIRONMENT}/csv")
MP3_DATA_FOLDER = os.path.join(BASE_DIR, f"data/{ENVIRONMENT}/mp3")
KEYS_FOLDER = os.path.join(BASE_DIR, "keys")
os.makedirs(CSV_DATA_FOLDER, exist_ok=True)
os.makedirs(MP3_DATA_FOLDER, exist_ok=True)
os.makedirs(KEYS_FOLDER, exist_ok=True)

DATABASE_URL = f"sqlite:///{DATABASES[ENVIRONMENT]}"

print(f"ENVIRONMENT (from .env): {ENVIRONMENT}")
print(f"Running in `{ENVIRONMENT.upper()}` environment")
print(f"Using database: {DATABASE_URL}")
print(f"CSV data folder: {CSV_DATA_FOLDER}")
print(f"MP3 data folder: {MP3_DATA_FOLDER}")
print(f"Keys folder: {KEYS_FOLDER}")