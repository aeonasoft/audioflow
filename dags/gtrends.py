from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
import os
import requests
import pandas as pd
import logging
import google.auth
from google.cloud import bigquery
import json
import re
import google.generativeai as genai
from sqlalchemy.exc import IntegrityError
from app.database import SessionLocal, init_db
from app.models.models import GoogleTrendsEnhanced
from utils.config import (
    CSV_DATA_FOLDER,
    GENAI_API_KEY,
    BIGQUERY_PROJECT_ID,
    BIGQUERY_CREDENTIALS_PATH,
)
import time
from jsonschema import validate
import jsonschema.exceptions


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
genai.configure(api_key=GENAI_API_KEY)

SCHEMA = {
    "type": "object",
    "properties": {
        "trends": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "term_en": {"type": "string"},
                    "description_en": {"type": "string"},
                    "description_de": {"type": "string"},
                    "description_fr": {"type": "string"},
                    "sentiment_score_en": {"type": "number"},
                    "link": {"type": "string"},
                    "pub_date": {"type": "string"},
                },
                "required": ["term_en", "description_en", "description_de", "description_fr", "sentiment_score_en", "link", "pub_date"],
            },
        }
    },
    "required": ["trends"],
}


def fetch_gtrends_data_from_bigquery():
    """Fetches Google Trends data from BigQuery and saves to CSV."""
    credentials, project_id = google.auth.load_credentials_from_file(BIGQUERY_CREDENTIALS_PATH)
    client = bigquery.Client(credentials=credentials, project=BIGQUERY_PROJECT_ID)

    query = """
    WITH Aggregated_Trends AS (
        SELECT
            term,
            SUM(score) AS total_score
        FROM
            `bigquery-public-data.google_trends.top_terms`
        WHERE
            refresh_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        GROUP BY
            term
    )
    SELECT
        term AS term_en,
        total_score,
        RANK() OVER (ORDER BY total_score DESC) AS ranking
    FROM
        Aggregated_Trends
    ORDER BY
        ranking
    LIMIT 25;
    """
    query_job = client.query(query)
    results = query_job.result()

    trends = [dict(row) for row in results]
    df = pd.DataFrame(trends)

    timestamp = datetime.now().strftime("%Y%m%d_%H_%M")
    filename = f"{timestamp}__gtrends.csv"
    os.makedirs(CSV_DATA_FOLDER, exist_ok=True)
    csv_path = os.path.join(CSV_DATA_FOLDER, filename)
    df.to_csv(csv_path, index=False)
    logging.info(f"SUCCESS: Google Trends data saved to: {csv_path}")


def get_latest_gtrends_file(enhanced=False):
    """Retrieve the most recent raw or enhanced Google Trends file."""
    suffix = "gtrends_enhanced.csv" if enhanced else "gtrends.csv"
    files = [f for f in os.listdir(CSV_DATA_FOLDER) if f.endswith(suffix)]
    if not files:
        logging.error(f"No Google Trends CSV files found (enhanced={enhanced}).")
        return None

    latest_file = max(files, key=lambda f: os.path.getmtime(os.path.join(CSV_DATA_FOLDER, f)))
    return os.path.join(CSV_DATA_FOLDER, latest_file)


def enhance_gtrends_data():
    """Enhances Google Trends data with AI-generated descriptions and sentiment analysis."""
    latest_file = get_latest_gtrends_file()
    if not latest_file:
        return

    df = pd.read_csv(latest_file)
    trends = df.head(15).to_dict(orient="records")
    
    enhanced_results = []
    max_retries = 1
    base_delay = 2

    for i in range(0, len(trends), 5):
        batch = trends[i : i + 5]

        prompt = f"""
        You are an AI trend analyst. Given trending search terms, generate descriptions, links, and sentiment analysis.

        **Instructions:**
        
        1. For each term, generate a clear and relevant description in English (description_en), German (description_de), and French (description_fr).
           - If the term is ambiguous or lacks context, provide a general description based on your knowledge.
        
        2. **Sentiment Analysis:**
           - Assign a **sentiment score (`sentiment_score_en`)** (-1 to 1) based on the **English description (`description_en`) only.**
           - **Positive (0.3 to 1.0):** Good news, achievements.
           - **Neutral (-0.3 to 0.3):** Regular updates, factual news.
           - **Negative (-1 to -0.3):** Disasters, controversies.
        
        3. Find a a most recent reputable news source link (link) related to this new trend and provide the publication date (pub_date).
           - If no recent news articles are available for the term, generate a placeholder link and publication date ("https://example.com" and "2025-01-01").
        
        4. Return JSON only in this format:
        {{
            "trends": [
                {{
                    "term_en": "Search term",
                    "description_en": "English description",
                    "description_de": "German description",
                    "description_fr": "French description",
                    "sentiment_score_en": sentiment_value,
                    "link": "News article URL",
                    "pub_date": "YYYY-MM-DD"
                }}
            ]
        }}

        **Search Terms:**
        {json.dumps([{"term_en": t["term_en"]} for t in batch], indent=2)}
        
        **Important: Ensure only valid JSON output and no extra text.**
        """

        for attempt in range(max_retries):
            try:
                # model = genai.GenerativeModel("gemini-pro")
                model = genai.GenerativeModel("gemini-1.5-flash")
                response = model.generate_content(prompt)
                response_text = response.text.strip()

                json_data = json.loads(re.search(r"{.*}", response_text, re.DOTALL).group(0))
                validate(instance=json_data, schema=SCHEMA)

                enhanced_results.extend(json_data["trends"])
                break
            except json.JSONDecodeError as e:
                logging.error(f"JSON decode error: {e}, Response: {response.text}")
                time.sleep(base_delay * (2 ** attempt))
            except jsonschema.exceptions.ValidationError as e:
                logging.error(f"Schema validation error: {e}, Data: {json_data}")
                break
            except requests.exceptions.RequestException as e:
                logging.error(f"API request error: {e}")
                time.sleep(base_delay * (2 ** attempt))
            except Exception as e:
                logging.error(f"Error enhancing batch: {e}")
                time.sleep(base_delay * (2 ** attempt))
        else:
            logging.error(f"Failed to enhance batch at index {i} after all retries.")

    enhanced_trends = []
    for i, trend in enumerate(trends):
        if i < len(enhanced_results):
            enhanced_trend = trend.copy()
            enhanced_trend.update(enhanced_results[i])
            enhanced_trends.append(enhanced_trend)
        else:
            enhanced_trend = trend.copy()
            enhanced_trend.update({
                "term_en": trend.get("term_en", ""), 
                "description_en": "",
                "description_de": "",
                "description_fr": "",
                "sentiment_score_en": 0.0,
                "link": "",
                "pub_date": "",
            })
            enhanced_trends.append(enhanced_trend)

    enhanced_df = pd.DataFrame(enhanced_trends)

    timestamp = datetime.now().strftime("%Y%m%d_%H_%M")
    filename = f"{timestamp}__gtrends_enhanced.csv"
    output_path = os.path.join(CSV_DATA_FOLDER, filename)
    enhanced_df.to_csv(output_path, index=False)
    logging.info(f"SUCCESS: Enhanced data saved to {output_path}")


init_db()

def ingest_gtrends_data():
    """Ingests enhanced Google Trends data into the database."""
    session = SessionLocal()
    latest_file = get_latest_gtrends_file(enhanced=True)
    if not latest_file:
        return

    df = pd.read_csv(latest_file)
    records = []
    skipped = 0

    existing_terms = {row.term_en for row in session.query(GoogleTrendsEnhanced.term_en).all()}

    for _, row in df.iterrows():

        if row.get("term_en") in existing_terms:
            logging.warning(f"Skipping duplicate term: {row['term_en']}")
            skipped += 1
            continue

        if pd.isna(row.get("description_en")) and pd.isna(row.get("description_de")) and pd.isna(row.get("description_fr")) and \
           pd.isna(row.get("sentiment_score_en")):
            logging.warning(f"Skipping empty enhancement row for term: {row['term_en']} (all enhancement fields empty)")
            skipped += 1
            continue

        record = GoogleTrendsEnhanced(
            term_en=row.get("term_en"),
            description_en=row.get("description_en"),
            description_de=row.get("description_de"),
            description_fr=row.get("description_fr"),
            sentiment_score_en=row.get("sentiment_score_en"),
            link=row.get("link"),
            pub_date=row.get("pub_date"),
        )
        records.append(record)

    try:
        session.add_all(records)
        session.commit()
        logging.info(f"SUCCESS: {len(records)} records ingested. ({skipped} skipped)")
    except IntegrityError as e:
        session.rollback()
        logging.error(f"IntegrityError: {e}")
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
    dag_id="gtrends",
    default_args={"owner": "airflow", "depends_on_past": False, "email_on_failure": False, "retries": 1, "retry_delay": timedelta(minutes=5),},
    schedule_interval="50 10 * * *", # afternoon + 5 min in future (given server time).
    start_date=datetime(2025, 2, 20), 
    catchup=False,
    tags=["gtrends", "pipeline"],
) as dag:
    
    fetch_task = PythonOperator(
        task_id="fetch_gtrends_data",
        python_callable=fetch_gtrends_data_from_bigquery,
    )

    enhance_task = PythonOperator(
        task_id="enhance_gtrends_data",
        python_callable=enhance_gtrends_data,
    )

    ingest_task = PythonOperator(
        task_id="ingest_gtrends_data",
        python_callable=ingest_gtrends_data,
    )

    failure_alert = EmailOperator(
        task_id="failure_alert",
        to="foobarbar@gmail.com", # Replace with own details
        subject="Google Trends DAG Failed",
        html_content="The Google Trends DAG failed. Please investigate.",
        trigger_rule="one_failed",  
    )
    
    fetch_task >> enhance_task >> ingest_task >> failure_alert