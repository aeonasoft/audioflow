from sqlalchemy.orm import Session
from database import SessionLocal, init_db
from models import AudioFile, Subscriber, Email


init_db()

def add_mock_records():
    db: Session = SessionLocal()
    try:
        mock_audio_files = [
            AudioFile(
                bbc_en="Ukraine not in NATO",
                bbc_de="Ukraine nicht in der NATO",
                bbc_fr="L'Ukraine pas dans l'OTAN",
                audio_file_en="/static/audio/20250207_11_20__en.mp3",
                audio_file_de="/static/audio/20250207_11_20__de.mp3",
                audio_file_fr="/static/audio/20250207_11_20__fr.mp3"
            ),
            AudioFile(
                bbc_en="New AI Regulations in EU",
                bbc_de="Neue KI-Regulierungen in der EU",
                bbc_fr="Nouvelles réglementations IA dans l'UE",
                audio_file_en="/static/audio/20250208_11_20__en.mp3",
                audio_file_de="/static/audio/20250208_11_20__de.mp3",
                audio_file_fr="/static/audio/20250208_11_20__fr.mp3"
            ),
            AudioFile(
                bbc_en="Tech Giants Announce Layoffs",
                bbc_de="Tech-Giganten kündigen Entlassungen an",
                bbc_fr="Les géants de la tech annoncent des licenciements",
                audio_file_en="/static/audio/20250209_11_20__en.mp3",
                audio_file_de="/static/audio/20250209_11_20__de.mp3",
                audio_file_fr="/static/audio/20250209_11_20__fr.mp3"
            )
        ]
        db.add_all(mock_audio_files)
        db.commit()
        
        audio_files = db.query(AudioFile).all()

        mock_subscribers = [
            Subscriber(name="Jo-ann", lang_preference="EN", email_address="joann@example.com"),
            Subscriber(name="Max", lang_preference="DE", email_address="max@example.com"),
            Subscriber(name="Sophie", lang_preference="FR", email_address="sophie@example.com")
        ]
        db.add_all(mock_subscribers)
        db.commit()

        subscribers = db.query(Subscriber).all()

        mock_emails = [
            Email(
                subscriber_id=subscribers[0].id,
                subject="🌞 Audio News & Trend Updates (2025-02-07)",
                body="Here is your latest audio news update.",
                status="success",
                sent_date="2025-02-07 10:30:00",
                audio_files=[audio_files[0]]
            ),
            Email(
                subscriber_id=subscribers[1].id,
                subject="🌞 Audio-Nachrichten & Trend-Updates (2025-02-08)",
                body="Hier ist Ihr neuestes Audio-Update.",
                status="success",
                sent_date="2025-02-08 10:30:00",
                audio_files=[audio_files[1]]
            ),
            Email(
                subscriber_id=subscribers[2].id,
                subject="🌞 Actualités Audio & Tendances (2025-02-09)",
                body="Voici votre dernière mise à jour audio.",
                status="failed",
                sent_date=None,
                audio_files=[audio_files[2]]
            )
        ]
        db.add_all(mock_emails)
        db.commit()

        print("Mock data added successfully.")

    except Exception as e:
        db.rollback()
        print(f"Error adding mock records: {e}")

    finally:
        db.close()

add_mock_records()