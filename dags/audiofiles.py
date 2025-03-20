from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
import logging
import os
import pandas as pd
from app.database import SessionLocal, init_db
from app.models.models import BBCArticleEnhanced, HackerNewsEnhanced, GoogleTrendsEnhanced, AudioFile
from utils.config import MP3_DATA_FOLDER, CSV_DATA_FOLDER
from gtts import gTTS


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

init_db()

def fetch_latest_articles(**context):
    """Fetches the most recent records per source based on RECORD_LIMIT."""
    RECORD_LIMIT = 5
    session = SessionLocal()
    
    def serialize_object(obj):
        """Converts an SQLAlchemy object to a serializable dictionary."""
        return {col.name: getattr(obj, col.name) for col in obj.__table__.columns}
    
    try:
        bbc_articles = session.query(BBCArticleEnhanced).order_by(BBCArticleEnhanced.create_date.desc()).limit(RECORD_LIMIT).all()
        hnews_articles = session.query(HackerNewsEnhanced).order_by(HackerNewsEnhanced.create_date.desc()).limit(RECORD_LIMIT).all()
        gtrends_terms = session.query(GoogleTrendsEnhanced).order_by(GoogleTrendsEnhanced.create_date.desc()).limit(RECORD_LIMIT).all()
        
        articles = {
            "bbc_articles": [serialize_object(a) for a in bbc_articles],
            "hnews_articles": [serialize_object(a) for a in hnews_articles],
            "gtrends_terms": [serialize_object(a) for a in gtrends_terms],
        }
        
        context['ti'].xcom_push(key="articles", value=articles)
        logging.info("SUCCESS: Articles fetched and serialized for XCom.")
    except Exception as e:
        logging.error(f"ERROR: Failed to fetch and serialize articles: {e}")
        raise
    finally:
        session.close()


def prepare_audio_text_task(**context):
    """Prepares text for TTS by concatenating title/term + description + sentiment."""
    articles = context['ti'].xcom_pull(key="articles")

    def format_text(index, title, description, sentiment, lang="en"):
        sentiment = sentiment if sentiment else "0.0"
        if lang == "en":
            return f"{index}. {title}; {description}; With Sentiment score of: {sentiment}"
        elif lang == "de":
            return f"{index}. {title}; {description}; Mit einem Stimmungswert von: {sentiment}"
        elif lang == "fr":
            return f"{index}. {title}; {description}; Avec un score de sentiment de: {sentiment}"
        return ""

    data = []
    for i, (b, h, g) in enumerate(zip(articles["bbc_articles"], articles["hnews_articles"], articles["gtrends_terms"]), start=1):
        data.append({
            "bbc_id": b.get("id") if b else None,
            "hnews_id": h.get("id") if h else None,
            "gtrends_id": g.get("id") if g else None,
            "bbc_en": format_text(i, b.get("title_en"), b.get("description_en"), b.get("sentiment_score_en"), "en") if b else "",
            "bbc_de": format_text(i, b.get("title_de"), b.get("description_de"), b.get("sentiment_score_en"), "de") if b else "",
            "bbc_fr": format_text(i, b.get("title_fr"), b.get("description_fr"), b.get("sentiment_score_en"), "fr") if b else "",
            "hnews_en": format_text(i, h.get("title_en"), h.get("description_en"), h.get("sentiment_score_en"), "en") if h else "",
            "hnews_de": format_text(i, h.get("title_de"), h.get("description_de"), h.get("sentiment_score_en"), "de") if h else "",
            "hnews_fr": format_text(i, h.get("title_fr"), h.get("description_fr"), h.get("sentiment_score_en"), "fr") if h else "",
            "gtrends_en": format_text(i, g.get("term_en"), g.get("description_en"), g.get("sentiment_score_en"), "en") if g else "",
            "gtrends_de": format_text(i, g.get("term_en"), g.get("description_de"), g.get("sentiment_score_en"), "de") if g else "",
            "gtrends_fr": format_text(i, g.get("term_en"), g.get("description_fr"), g.get("sentiment_score_en"), "fr") if g else "",
        })

    df = pd.DataFrame(data)
    context['ti'].xcom_push(key="prepared_text", value=df.to_dict(orient="records"))
    logging.info("SUCCESS: Prepared audio text and serialized DataFrame for XCom.")


def generate_script_task(**context):
    """Creates the structured script for the audio file from all sources."""
    prepared_text = context['ti'].xcom_pull(key="prepared_text")
    df = pd.DataFrame(prepared_text)

    date_str = datetime.now().strftime("%d %B %Y")
    
    welcome_msg_en = f"Good morning, today is {date_str}, and here follows your updates:\n\n"
    welcome_msg_de = f"Guten Morgen, heute ist {date_str}, und hier folgen Ihre Updates:\n\n"
    welcome_msg_fr = f"Bonjour, aujourd'hui nous sommes le {date_str}, et voici vos actualités:\n\n"

    closing_msg_en = "\n\nThank you for listening, have a nice day!\nThis news podcast was brought to you by D.Bosman Media Services."
    closing_msg_de = "\n\nDanke fürs Zuhören, einen schönen Tag!\nDieser Nachrichten-Podcast wurde Ihnen präsentiert von D.Bosman Media Services."
    closing_msg_fr = "\n\nMerci d'avoir écouté, passez une bonne journée!\nCe podcast d'actualités vous est présenté par D.Bosman Media Services."

    en_script = welcome_msg_en + "(A) Top search updates from Google Trends:\n" + "\n".join(df["gtrends_en"].dropna()) + \
                "\n\n(B) World updates from BBC News:\n" + "\n".join(df["bbc_en"].dropna()) + \
                "\n\n(C) Technology updates from Hacker News:\n" + "\n".join(df["hnews_en"].dropna()) + closing_msg_en
    
    de_script = welcome_msg_de + "(A) Top-Suchtrends von Google:\n" + "\n".join(df["gtrends_de"].dropna()) + \
                "\n\n(B) Welt-Nachrichten von BBC News:\n" + "\n".join(df["bbc_de"].dropna()) + \
                "\n\n(C) Technologie-Updates von Hacker News:\n" + "\n".join(df["hnews_de"].dropna()) + closing_msg_de
    
    fr_script = welcome_msg_fr + "(A) Mises à jour tendances Google Trends:\n" + "\n".join(df["gtrends_fr"].dropna()) + \
                "\n\n(B) Infos mondiales de BBC News:\n" + "\n".join(df["bbc_fr"].dropna()) + \
                "\n\n(C) Mises à jour technologiques de Hacker News:\n" + "\n".join(df["hnews_fr"].dropna()) + closing_msg_fr

    context['ti'].xcom_push(key="scripts", value={"en": en_script, "de": de_script, "fr": fr_script})
    logging.info("SUCCESS: Generated scripts and serialized for XCom.")


def create_audio(text, lang, filename):
    """Creates an audio file using gTTS."""
    try:
        tts = gTTS(text=text, lang=lang)
        tts.save(filename)
        logging.info(f"SUCCESS: Audio saved: {filename}")
        return filename
    except Exception as e:
        logging.error(f"ERROR: Error generating audio: {e}")
        return None


def generate_audio_files(**context):
    """Generate audio files from scripts."""
    scripts = context['ti'].xcom_pull(key="scripts")
    prepared_text = context['ti'].xcom_pull(key="prepared_text")
    df = pd.DataFrame(prepared_text)

    timestamp = datetime.now().strftime("%Y%m%d_%H_%M")

    audio_file_paths = {
        "audio_file_en": os.path.join(MP3_DATA_FOLDER, f"{timestamp}__en.mp3"),
        "audio_file_de": os.path.join(MP3_DATA_FOLDER, f"{timestamp}__de.mp3"),
        "audio_file_fr": os.path.join(MP3_DATA_FOLDER, f"{timestamp}__fr.mp3"),
    }

    df["audio_file_en"] = create_audio(scripts["en"], "en", audio_file_paths["audio_file_en"])
    df["audio_file_de"] = create_audio(scripts["de"], "de", audio_file_paths["audio_file_de"])
    df["audio_file_fr"] = create_audio(scripts["fr"], "fr", audio_file_paths["audio_file_fr"])

    context['ti'].xcom_push(key="prepared_dataframe", value=df.to_dict(orient="records"))
    context['ti'].xcom_push(key="audio_file_paths", value=audio_file_paths)
    logging.info("SUCCESS: Audio files generated and data pushed to XCom.")


def save_csv_task(**context):
    """Save the prepared DataFrame to a CSV file."""
    prepared_data = context['ti'].xcom_pull(key="prepared_dataframe")
    df = pd.DataFrame(prepared_data)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H_%M")
    all_csv_path = os.path.join(CSV_DATA_FOLDER, f"{timestamp}__all.csv")
    
    df.to_csv(all_csv_path, index=False)
    logging.info(f"SUCCESS: CSV saved at {all_csv_path}.")
    context['ti'].xcom_push(key="csv_path", value=all_csv_path)


def get_audiofile_schema():
    """Returns the standard schema for the DataFrame used in the DAG."""
    return [
        "bbc_id", "hnews_id", "gtrends_id",
        "bbc_en", "bbc_de", "bbc_fr",
        "hnews_en", "hnews_de", "hnews_fr",
        "gtrends_en", "gtrends_de", "gtrends_fr",
        "audio_file_en", "audio_file_de", "audio_file_fr"
    ]


def ingest_audio_data(**context):
    """Save audio file metadata to the database."""
    prepared_data = context['ti'].xcom_pull(key="prepared_dataframe")
    df = pd.DataFrame(prepared_data)

    expected_columns = get_audiofile_schema()
    if not all(col in df.columns for col in expected_columns):
        raise ValueError(f"ERROR: DataFrame does not match expected schema. Expected columns: {expected_columns}, but got: {list(df.columns)}")

    session = SessionLocal()
    try:
        session.bulk_insert_mappings(AudioFile, df.to_dict(orient="records"))
        session.commit()
        logging.info("SUCCESS: Audio files metadata stored successfully!")
    except Exception as e:
        session.rollback()
        logging.error(f"ERROR: Error inserting audio file metadata into database: {e}")
        raise
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
    dag_id="audiofiles",
    default_args={"owner": "airflow", "depends_on_past": False, "email_on_failure": False, "retries": 2, "retry_delay": timedelta(minutes=5),},
    description="A DAG to generate and ingest audio files",
    schedule_interval="50 10 * * *", # afternoon + 5 min in future (given server time).
    start_date=datetime(2025, 2, 20), 
    catchup=False,
    tags=["audiofiles", "pipeline"],
) as dag:

    bbc_sensor = ExternalTaskSensor(
        task_id="bbc_sensor",
        external_dag_id="bbcnews",
        allowed_states=["success"],
        external_task_id=None,
        timeout=3600,
        poke_interval=30,
        mode="poke",
    )

    hackernews_sensor = ExternalTaskSensor(
        task_id="hackernews_sensor",
        external_dag_id="hackernews",
        allowed_states=["success"],
        external_task_id=None,
        timeout=3600,
        poke_interval=30,
        mode="poke",
    )

    gtrends_sensor = ExternalTaskSensor(
        task_id="gtrends_sensor",
        external_dag_id="gtrends",
        allowed_states=["success"],
        external_task_id=None,
        timeout=3600,
        poke_interval=30,
        mode="poke",
    )

    fetch_task = PythonOperator(
        task_id="fetch_latest_articles",
        python_callable=fetch_latest_articles,
        provide_context=True,
    )

    prepare_task = PythonOperator(
        task_id="prepare_audio_text",
        python_callable=prepare_audio_text_task,
        provide_context=True,
    )

    script_task = PythonOperator(
        task_id="generate_script",
        python_callable=generate_script_task,
        provide_context=True,
    )

    audio_task = PythonOperator(
        task_id="generate_audio_files",
        python_callable=generate_audio_files,
        provide_context=True,
    )

    csv_task = PythonOperator(
        task_id="save_csv",
        python_callable=save_csv_task,
        provide_context=True,
    )

    ingest_task = PythonOperator(
        task_id="ingest_audio_data",
        python_callable=ingest_audio_data,
        provide_context=True,
    )

    failure_alert = EmailOperator(
        task_id="failure_alert",
        to="foobarbar@gmail.com", # Replace with own details
        subject="Audiofiles DAG Failed",
        html_content="The Audiofiles DAG failed. Please investigate.",
        trigger_rule="one_failed",
    )

    [bbc_sensor, hackernews_sensor, gtrends_sensor] >> fetch_task >> prepare_task >> script_task >> audio_task >> csv_task >> ingest_task >> failure_alert