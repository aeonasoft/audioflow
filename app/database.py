import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.models.base import Base
from utils.config import DATABASE_URL, ENVIRONMENT, CSV_DATA_FOLDER, MP3_DATA_FOLDER


db_dir = os.path.dirname(DATABASE_URL.replace("sqlite:///", ""))
if not os.path.exists(db_dir):
    os.makedirs(db_dir)

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def init_db():
    from app.models import models
    print("Models registered:", [
        models.BBCArticleEnhanced.__tablename__,
        models.HackerNewsEnhanced.__tablename__,
        models.GoogleTrendsEnhanced.__tablename__,
        models.AudioFile.__tablename__,
        models.Subscriber.__tablename__,
        models.Email.__tablename__,
        "email_audio_association"
    ])
    print("email_audio_association columns:", models.email_audio_association.c.keys())
    Base.metadata.create_all(bind=engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
        
        
if __name__ == "__main__":
    print(f"Running in `{ENVIRONMENT.upper()}` environment")
    print(f"Using database: {DATABASE_URL}")
    print(f"Raw data folder: {CSV_DATA_FOLDER}")
    print(f"Raw data folder: {MP3_DATA_FOLDER}")
    init_db()
    print("Database initialized successfully!")