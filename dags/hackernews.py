from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
import os
import requests
import pandas as pd
import logging
import json
import time
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql import func
from app.database import SessionLocal, init_db
from app.models.models import HackerNewsEnhanced
from utils.config import (
    CSV_DATA_FOLDER,
    GENAI_API_KEY,
    HACKERNEWS_TOP_STORIES_URL,
    HACKERNEWS_ITEM_URL,
)
from jsonschema import validate
import google.generativeai as genai
import jsonschema.exceptions
import re


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


def fetch_hackernews_articles():
    """Fetch top Hacker News stories and save to CSV."""
    response = requests.get(HACKERNEWS_TOP_STORIES_URL)
    if response.status_code != 200:
        raise Exception("Failed to fetch top stories from Hacker News")

    story_ids = response.json()[:30]
    news_items = []
    for story_id in story_ids:
        story_response = requests.get(HACKERNEWS_ITEM_URL.format(story_id))
        if story_response.status_code == 200:
            story_data = story_response.json()
            news_items.append({
                "hn_id": story_data.get("id"),
                "rank": story_ids.index(story_id) + 1,
                "title_en": story_data.get("title", ""),
                "text_en": story_data.get("text", ""),
                "pub_date": datetime.fromtimestamp(
                    story_data.get("time", datetime.now().timestamp())
                ).strftime("%Y-%m-%d %H:%M:%S"),
                "link": story_data.get("url", ""),
                "score": story_data.get("score", 0),
                "comment_count": story_data.get("descendants", 0),
            })

    timestamp = datetime.now().strftime("%Y%m%d_%H_%M")
    filename = f"{timestamp}__hackernews.csv"
    os.makedirs(CSV_DATA_FOLDER, exist_ok=True)
    csv_path = os.path.join(CSV_DATA_FOLDER, filename)
    pd.DataFrame(news_items).to_csv(csv_path, index=False)
    logging.info(f"SUCCESS: Hacker News data saved to: {csv_path}")


def get_latest_hackernews_file(enhanced=False):
    """Retrieve the most recent raw or enhanced Hacker News file."""
    suffix = "hackernews_enhanced.csv" if enhanced else "hackernews.csv"
    files = [f for f in os.listdir(CSV_DATA_FOLDER) if f.endswith(suffix)]
    if not files:
        logging.error(f"No Hacker News CSV files found (enhanced={enhanced}).")
        return None

    latest_file = max(files, key=lambda f: os.path.getmtime(os.path.join(CSV_DATA_FOLDER, f)))
    return os.path.join(CSV_DATA_FOLDER, latest_file)


def enhance_hackernews_articles():
    """Enhance Hacker News articles using Gemini API."""
    latest_file = get_latest_hackernews_file()
    if not latest_file:
        return

    df = pd.read_csv(latest_file)
    articles = df.head(15).to_dict(orient="records")

    enhanced_results = []
    max_retries = 1
    base_delay = 2
    batch_size = 5

    for i in range(0, len(articles), batch_size):
        batch = articles[i:i + batch_size]
        prompt = f"""
                You are an AI news analyst. Given Hacker News articles, generate descriptions and analyze sentiment.

                **Instructions:**
                1. Use the **title_en** to generate three new descriptions in english (description_en), german (description_de), and french (description_fr).
                   - If the **link** is available, you can use it for more context, but if not, skip it.
                2. Translate the **title_en** into german (title_de) and french (title_fr).
                3. **Assign a sentiment score** (-1 to 1) based on the English description.
                4. **Return JSON only** in this format:
                {{
                    "articles": [
                        {{
                            "title_de": "German title",
                            "title_fr": "French title",
                            "description_en": "Generated English description",
                            "description_de": "Generated German description",
                            "description_fr": "Generated French description",
                            "sentiment_score_en": sentiment_value
                        }}
                    ]
                }}

                Articles: {json.dumps(batch, indent=2)}
                """

        for attempt in range(max_retries):
            try:
                # model = genai.GenerativeModel("gemini-pro")
                model = genai.GenerativeModel("gemini-1.5-flash")
                response = model.generate_content(prompt)
                response_text = response.text.strip()
                json_data = json.loads(re.search(r"{.*}", response_text, re.DOTALL).group(0))

                validate(instance=json_data, schema=SCHEMA)
                enhanced_results.extend(json_data.get("articles", []))
                break
            except (json.JSONDecodeError, jsonschema.exceptions.ValidationError) as e:
                logging.error(f"Error parsing or validating JSON: {e}")
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
    filename = f"{timestamp}__hackernews_enhanced.csv"
    output_path = os.path.join(CSV_DATA_FOLDER, filename)
    enhanced_df.to_csv(output_path, index=False, encoding="utf-8")
    logging.info(f"SUCCESS: Enhanced Hacker News saved to: {output_path}")


def ingest_hackernews_articles():
    """Ingest enhanced Hacker News articles into the database."""
    session = SessionLocal()
    latest_file = get_latest_hackernews_file(enhanced=True)
    if not latest_file:
        return

    df = pd.read_csv(latest_file)
    records = []
    skipped = 0

    existing_hn_ids = {row.hn_id for row in session.query(HackerNewsEnhanced.hn_id).all()}

    for _, row in df.iterrows():

        if row.get("hn_id") in existing_hn_ids:
            logging.warning(f"Skipping duplicate Hacker News ID: {row['hn_id']}")
            skipped += 1
            continue

        if pd.isna(row.get("title_de")) and pd.isna(row.get("title_fr")) and pd.isna(row.get("description_en")) and \
           pd.isna(row.get("description_de")) and pd.isna(row.get("description_fr")) and pd.isna(row.get("sentiment_score_en")):
            logging.warning(f"Skipping empty enhancement row for Hacker News ID: {row['hn_id']} (all fields empty)")
            skipped += 1
            continue

        record = HackerNewsEnhanced(
            hn_id=row["hn_id"],
            pub_date=row["pub_date"],
            rank=row["rank"],
            link=row["link"],
            title_en=row["title_en"],
            text_en=row["text_en"],
            score=row["score"],
            comment_count=row["comment_count"],
            title_de=row.get("title_de"),
            title_fr=row.get("title_fr"),
            description_en=row.get("description_en"),
            description_de=row.get("description_de"),
            description_fr=row.get("description_fr"),
            sentiment_score_en=row.get("sentiment_score_en", 0.0),
            create_date=func.now(),
        )
        records.append(record)

    try:
        session.add_all(records)
        session.commit()
        logging.info(f"SUCCESS: {len(records)} Hacker News articles ingested successfully! ({skipped} skipped)")
    except IntegrityError as e:
        session.rollback()
        logging.error(f"ERROR: IntegrityError while ingesting data: {e}")
    finally:
        session.close()

init_db()

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
    dag_id="hackernews",
    default_args={"owner": "airflow", "depends_on_past": False, "email_on_failure": False, "retries": 1, "retry_delay": timedelta(minutes=5),},
    schedule_interval="50 10 * * *", # afternoon + 5 min in future (given server time).
    start_date=datetime(2025, 2, 20), 
    catchup=False,
    tags=["hackernews", "pipeline"],
) as dag:
    
    fetch_task = PythonOperator(
        task_id="fetch_hackernews_articles",
        python_callable=fetch_hackernews_articles,
    )

    enhance_task = PythonOperator(
        task_id="enhance_hackernews_articles",
        python_callable=enhance_hackernews_articles,
    )

    ingest_task = PythonOperator(
        task_id="ingest_hackernews_articles",
        python_callable=ingest_hackernews_articles,
    )

    failure_alert = EmailOperator(
        task_id="failure_alert",
        to="foobarbar@gmail.com", # Replace with own details
        subject="Hackernews DAG Failed",
        html_content="The Hackernews DAG failed. Please investigate.",
        trigger_rule="one_failed",
    )
    
    fetch_task >> enhance_task >> ingest_task >> failure_alert
