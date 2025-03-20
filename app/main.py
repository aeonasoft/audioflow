import logging
from fastapi import FastAPI, Request, Depends, status
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from jinja2 import Environment, FileSystemLoader
from app.database import SessionLocal, init_db, get_db
from app.models.models import (
    BBCArticleEnhanced,
    HackerNewsEnhanced,
    GoogleTrendsEnhanced,
    AudioFile,
    Subscriber,
    Email
)
from app.models.schemas import (
    BBCArticleEnhancedSchema,
    HackerNewsEnhancedSchema,
    GoogleTrendsEnhancedSchema,
    AudioFileSchema,
    SubscriberSchema,
    EmailSchema
)
from utils.config import ENVIRONMENT


logging.basicConfig(level=logging.ERROR)

init_db()

app = FastAPI(title=f"Audioflow API - {ENVIRONMENT.upper()} Environment", version="1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

templates = Environment(loader=FileSystemLoader("app/templates"))

app.mount("/static", StaticFiles(directory="app/static"), name="static")


@app.get("/", response_class=HTMLResponse)
async def landing_page(request: Request):
    """Serve the landing page."""
    template = templates.get_template("landing_page.html")
    return HTMLResponse(content=template.render(request=request))


@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return RedirectResponse(url="/static/images/favicon.ico")


@app.post("/subscribe", status_code=status.HTTP_201_CREATED)
async def subscribe(subscribe_request: SubscriberSchema):
    """
    Handle subscription form submission. Validates and saves the data to the database.
    """
    db = SessionLocal()
    try:
        subscriber = Subscriber(
            name=subscribe_request.name,
            lang_preference=subscribe_request.lang_preference,
            email_address=subscribe_request.email_address
        )
        db.add(subscriber)
        db.commit()
        return {"status": "success", "message": "😃 Subscription successful!"}

    except IntegrityError:
        db.rollback()
        return {"status": "error", "message": "😢 Sorry, the record already exists or was not successfully submitted. Please try again."}

    except Exception as e:
        db.rollback()
        logging.error(f"Unexpected error occurred during subscription: {e}")
        return {"status": "error", "message": "😢 An unexpected error occurred. Please try again later."}

    finally:
        db.close()


@app.get("/bbc_articles", response_model=list[BBCArticleEnhancedSchema])
def fetch_bbc_articles(db: Session = Depends(get_db)):
    """Fetch all BBC enhanced articles."""
    return db.query(BBCArticleEnhanced).all()


@app.get("/hnews_articles", response_model=list[HackerNewsEnhancedSchema])
def fetch_hnews_articles(db: Session = Depends(get_db)):
    """Fetch all HackerNews enhanced articles."""
    return db.query(HackerNewsEnhanced).all()


@app.get("/google_trends", response_model=list[GoogleTrendsEnhancedSchema])
def fetch_google_trends(db: Session = Depends(get_db)):
    """Fetch all Google Trends enhanced terms."""
    return db.query(GoogleTrendsEnhanced).all()


@app.get("/audio_files", response_model=list[AudioFileSchema])
def fetch_audio_files(db: Session = Depends(get_db)):
    """Fetch all stored audio files."""
    return db.query(AudioFile).all()


@app.get("/subscribers", response_model=list[SubscriberSchema])
def fetch_subscribers(db: Session = Depends(get_db)):
    """Fetch all subscribers."""
    return db.query(Subscriber).all()


@app.get("/emails", response_model=list[EmailSchema])
def fetch_emails(db: Session = Depends(get_db)):
    """Fetch all sent emails."""
    return db.query(Email).all()