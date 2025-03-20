from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
import os
import logging
import smtplib
import pandas as pd
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from sqlalchemy import func
from app.database import SessionLocal, init_db
from app.models.models import Email, AudioFile, Subscriber
from utils.config import SMTP_SERVER, SMTP_PORT, SMTP_USERNAME, SMTP_PASSWORD, SMTP_SENDER_EMAIL, CSV_DATA_FOLDER


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

init_db()

# Replace emails with own email to default to. Testing purposes only.
SUBSCRIBERS = [
    {"name": "Jo-ann", "lang_preference": "EN", "subs_date": "2025-02-06", "email_address": "foobarbar@gmail.com"},
    {"name": "Max", "lang_preference": "DE", "subs_date": "2025-02-06", "email_address": "foobarbar@gmail.com"},
    {"name": "Sophie", "lang_preference": "FR", "subs_date": "2025-02-06", "email_address": "foobarbar@gmail.com"},
]

def fetch_latest_audio_filepath(lang):
    """Retrieves the most recent audio file path based on language preference."""
    session = SessionLocal()
    try:
        audio_file = session.query(AudioFile).order_by(AudioFile.create_date.desc()).first()
        if not audio_file:
            logging.error(f"ERROR: No audio files found for language: {lang}")
            return None, None

        file_path = getattr(audio_file, f"audio_file_{lang.lower()}", None)
        if not file_path or not os.path.exists(file_path):
            logging.error(f"ERROR: Missing audio file for {lang}: {file_path}")
            return None, None

        return file_path, audio_file.id
    finally:
        session.close()


def fetch_subscribers():
    """Fetch active subscribers from the database."""
    session = SessionLocal()
    try:
        subscribers = session.query(Subscriber).all()
        return subscribers
    finally:
        session.close()


def create_and_send_email(subscriber, audio_file, latest_audio_date):
    """Constructs and sends an email with an audio file attachment."""

    recipient = subscriber["email_address"] if isinstance(subscriber, dict) else subscriber.email_address
    name = subscriber["name"] if isinstance(subscriber, dict) else subscriber.name
    lang = subscriber["lang_preference"] if isinstance(subscriber, dict) else subscriber.lang_preference

    subject_map = {
        "EN": f"🌞 Audio News & Trend Updates ({latest_audio_date})",
        "DE": f"🌞 Audio-Nachrichten & Trend-Updates ({latest_audio_date})",
        "FR": f"🌞 Actualités Audio & Tendances ({latest_audio_date})"
    }
    subject = subject_map.get(lang, subject_map["EN"])

    body_map = {
        "EN": f"Good Morning {name},\n\nAttached is your latest audio news update. Have a great day!\n\nBest,\nDirk\n\n**Made with ❤️ by DB Media Services**",
        "DE": f"Guten Morgen {name},\n\nAnbei finden Sie Ihr neuestes Audio-News-Update. Einen schönen Tag!\n\nBeste Grüße,\nDirk\n\n**Made with ❤️ by DB Media Services**",
        "FR": f"Bonjour {name},\n\nCi-joint votre dernière mise à jour des actualités audio. Passez une bonne journée !\n\nCordialement,\nDirk\n\n**Made with ❤️ by DB Media Services**"
    }
    body = body_map.get(lang, body_map["EN"])

    msg = MIMEMultipart()
    msg["From"] = f"DB Media Services <{SMTP_SENDER_EMAIL}>"
    msg["To"] = recipient
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    if not os.path.exists(audio_file):
        logging.error(f"ERROR: Missing audio file: {audio_file}")
        return False

    try:
        with open(audio_file, "rb") as attachment:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", f"attachment; filename={os.path.basename(audio_file)}")
            msg.attach(part)
    except Exception as e:
        logging.error(f"ERROR: Failed to attach audio file: {e}")
        return False

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USERNAME, SMTP_PASSWORD)
            server.sendmail(SMTP_SENDER_EMAIL, recipient, msg.as_string())
            logging.info(f"SUCCESS: Email sent to {recipient}")
        return True
    except Exception as e:
        logging.error(f"ERROR: Email failed for {recipient}: {e}")
        return False


def log_sent_email_to_db(session, subscriber, sent_status, audio_files, latest_audio_date):
    """Logs email sending details in the database, including all related audio file IDs."""
    try:
        subscriber_id = subscriber["id"] if isinstance(subscriber, dict) else subscriber.id
        email_entry = Email(
            subscriber_id=subscriber_id,
            subject=f"🌞 Audio News & Trend Updates ({latest_audio_date})",
            body=f"Email sent to {subscriber['email_address']}" if isinstance(subscriber, dict) else f"Email sent to {subscriber.email_address}",
            status="success" if sent_status else "failed",
            sent_date=datetime.now().strftime("%Y-%m-%d %H:%M:%S") if sent_status else None
        )

        email_entry.audio_files.extend(audio_files)
        session.add(email_entry)
        session.commit()

        logging.info(
            f"SUCCESS: Email logged for {subscriber['email_address']}" 
            if isinstance(subscriber, dict) 
            else f"SUCCESS: Email logged for {subscriber.email_address}"
        )

        return {
            "email_id": email_entry.id,
            "subscriber_id": subscriber_id,
            "status": email_entry.status,
            "sent_date": email_entry.sent_date,
            "subject": email_entry.subject
        }
    
    except Exception as e:
        session.rollback()
        logging.error(f"ERROR: Failed to log email: {e}")
        return None


def process_and_send_all_emails(**context):
    """Processes and sends emails while logging all relevant audio file IDs."""
    session = SessionLocal()
    email_logs = []

    try:
        latest_create_date = session.query(func.max(AudioFile.create_date)).scalar()
        latest_audio_files = session.query(AudioFile).filter(AudioFile.create_date == latest_create_date).all()
    
        if not latest_audio_files:
            logging.warning("WARNING: No recent audio files found. Skipping email send process.")
            return

        latest_audio_date = latest_create_date.split(" ")[0]
        subscribers = session.query(Subscriber).all()
        
        if not subscribers:
            logging.warning("WARNING: No subscribers found in DB. Using fallback list.")
            subscribers = [Subscriber(**s) for s in SUBSCRIBERS]

        for subscriber in subscribers:

            lang_pref = subscriber["lang_preference"] if isinstance(subscriber, dict) else subscriber.lang_preference
            audio_file = getattr(latest_audio_files[0], f"audio_file_{lang_pref.lower()}")


            if audio_file:
                sent_status = create_and_send_email(subscriber, audio_file, latest_audio_date)
                email_log = log_sent_email_to_db(session, subscriber, sent_status, latest_audio_files, latest_audio_date)
                if email_log:
                    email_logs.append(email_log)

        context['ti'].xcom_push(key="email_logs", value=email_logs)
    
    finally:
        session.close()


def save_sent_emails_to_csv(**context):
    """Saves a CSV file listing all emails sent on the day the DAG ran."""
    email_logs = context['ti'].xcom_pull(key="email_logs")
    if not email_logs:
        logging.warning("WARNING: No emails were sent, skipping CSV generation.")
        return

    df = pd.DataFrame(email_logs)

    timestamp = datetime.now().strftime("%Y%m%d_%H_%M")
    email_csv_path = os.path.join(CSV_DATA_FOLDER, f"{timestamp}__emailed.csv")
    
    df.to_csv(email_csv_path, index=False)
    logging.info(f"SUCCESS: Email log saved at {email_csv_path}.")
    context['ti'].xcom_push(key="email_csv_path", value=email_csv_path)

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
    dag_id="emails",
    default_args={"owner": "airflow", "depends_on_past": False, "email_on_failure": False, "retries": 2, "retry_delay": timedelta(minutes=5),},
    description="A DAG to send emails with audio news files",
    schedule_interval="50 10 * * *", # afternoon + 5 min in future (given server time).
    start_date=datetime(2025, 2, 20), 
    catchup=False,
    tags=["emails", "pipeline"],
) as dag:

    audiofiles_sensor = ExternalTaskSensor(
        task_id="audiofiles_sensor",
        external_dag_id="audiofiles",
        allowed_states=["success"],
        external_task_id=None,
        timeout=3600,
        poke_interval=30,
        mode="poke",
    )
    
    send_emails_task = PythonOperator(
        task_id="process_and_send_all_emails",
        python_callable=process_and_send_all_emails,
        provide_context=True,
    )

    save_email_csv_task = PythonOperator(
        task_id="save_sent_emails_to_csv",
        python_callable=save_sent_emails_to_csv,
        provide_context=True,
    )
    
    failure_alert = EmailOperator(
        task_id="failure_alert",
        to="foobarbar@gmail.com", # Replace with own details
        subject="Audiofiles DAG Failed",
        html_content="The Audiofiles DAG failed. Please investigate.",
        trigger_rule="one_failed",
    )
	
    [audiofiles_sensor] >> send_emails_task >> save_email_csv_task >> failure_alert