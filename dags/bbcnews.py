from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
import os
import requests
import pandas as pd
from bs4 import BeautifulSoup
import logging
import json
import re
import google.generativeai as genai
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql import func
from app.database import SessionLocal, init_db
from app.models.models import BBCArticleEnhanced
from utils.config import CSV_DATA_FOLDER, BBC_RSS_URL, GENAI_API_KEY
from jsonschema import validate
import jsonschema.exceptions
import time


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
genai.configure(api_key=GENAI_API_KEY)

SCHEMA = {
    "type": "object",
    "properties": {
        "articles": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "title_de": {"type": "string"},
                    "title_fr": {"type": "string"},
                    "description_en": {"type": "string"},
                    "description_de": {"type": "string"},
                    "description_fr": {"type": "string"},
                    "sentiment_score_en": {"type": "number"},
                },
                "required": [
                    "title_de",
                    "title_fr",
                    "description_en",
                    "description_de",
                    "description_fr",
                    "sentiment_score_en",
                ],
            },
        }
    },
    "required": ["articles"],
}

def fetch_bbc_articles():
    """Fetches and parses BBC News RSS feed, saving data to CSV."""
    response = requests.get(BBC_RSS_URL)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, "lxml-xml")
        news_items = []
        for item in soup.find_all("item"):
            title_en = item.find("title").text if item.find("title") else ""
            description_en = item.find("description").text if item.find("description") else ""
            link = item.find("link").text if item.find("link") else ""
            pub_date = item.find("pubDate").text if item.find("pubDate") else ""
            media_thumbnail = item.find("media:thumbnail")
            media_url = media_thumbnail["url"] if media_thumbnail else ""
            news_items.append({
                "link": link,
                "title_en": title_en,
                "description_en": description_en,
                "pub_date": pub_date,
                "media_url": media_url
            })
        df = pd.DataFrame(news_items)
        timestamp = datetime.now().strftime("%Y%m%d_%H_%M")
        filename = f"{timestamp}__bbcnews.csv"
        os.makedirs(CSV_DATA_FOLDER, exist_ok=True)
        csv_path = os.path.join(CSV_DATA_FOLDER, filename)
        df.to_csv(csv_path, index=False)
        logging.info(f"SUCCESS: BBC News data saved to: {csv_path}")
    else:
        logging.error(f"ERROR: Failed to fetch RSS feed: {response.status_code}")
        raise Exception("Failed to fetch RSS feed")


def get_latest_bbc_news_file(enhanced=False):
    """Retrieve the most recent raw or enhanced BBC News file."""
    suffix = "bbcnews_enhanced.csv" if enhanced else "bbcnews.csv"
    files = [f for f in os.listdir(CSV_DATA_FOLDER) if f.endswith(suffix)]
    if not files:
        logging.error(f"No BBC News CSV files found (enhanced={enhanced}).")
        return None

    latest_file = max(files, key=lambda f: os.path.getmtime(os.path.join(CSV_DATA_FOLDER, f)))
    return os.path.join(CSV_DATA_FOLDER, latest_file)


def enhance_bbc_articles():
    """Enhances BBC articles with translations and sentiment analysis from Gemini API."""
    latest_file = get_latest_bbc_news_file()
    if not latest_file:
        return
    df = pd.read_csv(latest_file)
    articles = df.head(15).to_dict(orient="records")

    enhanced_results = []
    max_retries = 1
    base_delay = 2

    for i in range(0, len(articles), 5):
        batch = articles[i:i + 5]
        logging.info(f"Batch being sent to Gemini API: {json.dumps(batch, indent=2)}")

        prompt = f"""
        You are an AI news analyst. Given BBC news articles, translate them and analyze sentiment.

        **Instructions:**

        1. **Language-Specific Translations Of The Article's Title:**
           - Translate the English title (`title_en`) to a French title (`title_fr`) and a German title (`title_de`).

        2. **English Description Handling:**
           - If the English description (`description_en`) exists, use it as is. If the English description (`description_en`) does not exist, generate a concise summary by using the English title (`title_en`) and use that as the `description_en`. 
           - Given one of the two outcomes above, thereafter please translate the English description (`description_en`) to a French description (`description_fr`) and a German description (`description_de`).   
           - **Crucially:** Ensure that the generated French and German descriptions are actual translations of the provided English description. Do not generate new content in those languages.

        3. **Validate translations**:
           - Ensure that `description_en`, `description_fr`, and `description_de` are consistent in meaning and language.
           - If any translations fail or mismatch, mark the error explicitly in the output JSON.

        4. **Sentiment Analysis:**
           - Assign a **sentiment score (`sentiment_score_en`)** (-1 to 1) based on the **English description (`description_en`) only.**
           - **Positive (0.3 to 1.0):** Good news, achievements.
           - **Neutral (-0.3 to 0.3):** Regular updates, factual news.
           - **Negative (-1 to -0.3):** Disasters, controversies.
        
        5. **Return JSON only** with this format:
        {{
            "articles": [
                {{
                    "title_de": "German title",
                    "title_fr": "French title",
                    "description_en": "English description",
                    "description_de": "Generated German description",
                    "description_fr": "Generated French description",
                    "sentiment_score_en": sentiment_value
                }}
            ]
        }}
        **BBC News Articles (English):**
        {json.dumps(batch, indent=2)}

        **Important: Double-check that the French and German descriptions are accurate translations of the provided English description. Ensure only valid JSON output and no extra text.**
        """
        
        for attempt in range(max_retries):
            try:
                # model = genai.GenerativeModel("gemini-pro")
                model = genai.GenerativeModel("gemini-1.5-flash")
                response = model.generate_content(prompt)
                response_text = response.text.strip()

                logging.info(f"Raw API response: {response_text}")

                json_data = json.loads(re.search(r"{.*}", response_text, re.DOTALL).group(0))
                validate(instance=json_data, schema=SCHEMA)

                enhanced_results.extend(json_data.get("articles", []))

                break
            except (json.JSONDecodeError, jsonschema.exceptions.ValidationError) as e:
                logging.error(f"JSON validation or parsing error: {e}")
            except Exception as e:
                logging.error(f"Error during API call (attempt {attempt + 1}): {e}")
                time.sleep(base_delay * (2 ** attempt))
        else:
            logging.error(f"Failed to enhance batch starting at index {i} after retries.")

    for i, article in enumerate(articles):
        if i < len(enhanced_results):
            article.update(enhanced_results[i])

    enhanced_df = pd.DataFrame(articles)
    timestamp = datetime.now().strftime("%Y%m%d_%H_%M")
    filename = f"{timestamp}__bbcnews_enhanced.csv"
    output_path = os.path.join(CSV_DATA_FOLDER, filename)
    enhanced_df.to_csv(output_path, index=False, encoding="utf-8")
    logging.info(f"SUCCESS: Enhanced BBC News saved to: {output_path}")


init_db()

def ingest_bbc_articles():
    """Ingests enhanced BBC News articles directly into the database."""
    session = SessionLocal()
    latest_file = get_latest_bbc_news_file(enhanced=True)
    if not latest_file:
        return
    df = pd.read_csv(latest_file)
    records = []
    skipped = 0

    existing_links = {row.link for row in session.query(BBCArticleEnhanced.link).all()}

    for _, row in df.iterrows():
        
        if row.get("link") in existing_links:
            logging.warning(f"Skipping duplicate link: {row['link']}")
            skipped += 1
            continue

        if pd.isna(row["title_de"]) and pd.isna(row["title_fr"]) and pd.isna(row["description_en"]) and \
           pd.isna(row["description_de"]) and pd.isna(row["description_fr"]) and pd.isna(row["sentiment_score_en"]):
            logging.warning(f"WARNING: Skipping empty enhancement row for link: {row['link']} (all fields empty)")
            skipped += 1
            continue

        sentiment_score = row.get("sentiment_score_en", 0.0) or 0.0
        article_enhanced = BBCArticleEnhanced(
            link=row.get("link", None),
            title_en=row.get("title_en", None),
            description_en=row.get("description_en", None),
            pub_date=row.get("pub_date", None),
            media_url=row.get("media_url", None),
            title_de=row.get("title_de", None),
            title_fr=row.get("title_fr", None),
            description_de=row.get("description_de", None),
            description_fr=row.get("description_fr", None),
            sentiment_score_en=sentiment_score,
            create_date=func.now(),
        )
        records.append(article_enhanced)

    try:
        session.add_all(records)
        session.commit()
        logging.info(f"SUCCESS: {len(records)} enhanced BBC News articles ingested successfully! ({skipped} skipped)")
    except IntegrityError as e:
        session.rollback()
        logging.error(f"ERROR: IntegrityError while ingesting enhanced BBC articles: {e}")
    finally:
        session.close()

# ----------------------------------------------------------------------------------------
# DAG Testing:
# ----------------------------------------------------------------------------------------
# Step 1: Old State = Paused
    # schedule_interval="55 23 * * *", # nothing close to current server time.
    # start_date=datetime(2025, 2, 20), # yesterday (or more).
# Step 2: Unpaused all DAGs
# Step 3: New State = Unpaused
    # schedule_interval="0 10 * * *", # morning + 5 min in future (given server time).
    # schedule_interval="35 13 * * *", # afternoon + 5 min in future (given server time).
    # schedule_interval="50 21 * * *", # evening + 5 min in future (given server time).
    # start_date=datetime(2025, 2, 20), # yesterday (or more).
# ----------------------------------------------------------------------------------------    

with DAG(
    dag_id="bbcnews",
    default_args={"owner": "airflow", "depends_on_past": False, "email_on_failure": False, "retries": 1, "retry_delay": timedelta(minutes=5),},
    schedule_interval="50 10 * * *", # afternoon + 5 min in future (given server time).
    start_date=datetime(2025, 2, 20), 
    catchup=False,
    tags=["bbc", "pipeline"],
) as dag:
    
    fetch_task = PythonOperator(
        task_id="fetch_bbc_articles",
        python_callable=fetch_bbc_articles,
    )

    enhance_task = PythonOperator(
        task_id="enhance_bbc_articles",
        python_callable=enhance_bbc_articles,
    )

    ingest_task = PythonOperator(
        task_id="ingest_bbc_articles",
        python_callable=ingest_bbc_articles,
    )

    failure_alert = EmailOperator(
        task_id="failure_alert",
        to="foobarbar@gmail.com", # Replace with own details
        subject="BBC News DAG Failed",
        html_content="The BBC News DAG failed. Please investigate.",
        trigger_rule="one_failed",  
    )
    
    fetch_task >> enhance_task >> ingest_task >> failure_alert