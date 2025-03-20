from sqlalchemy import Column, Integer, Text, Float, ForeignKey, Table, func
from sqlalchemy.orm import relationship
from app.models.base import Base


class BBCArticleEnhanced(Base):
    __tablename__ = "bbc_articles_enhanced"

    id = Column(Integer, primary_key=True, autoincrement=True)
    link = Column(Text, nullable=True)
    title_en = Column(Text, nullable=True)
    description_en = Column(Text, nullable=True)
    pub_date = Column(Text, nullable=True)
    media_url = Column(Text, nullable=True)
    title_de = Column(Text, nullable=True)
    title_fr = Column(Text, nullable=True)
    description_de = Column(Text, nullable=True)
    description_fr = Column(Text, nullable=True)
    sentiment_score_en = Column(Float, nullable=True)
    create_date = Column(Text, nullable=False, server_default=func.now())
    audio_files = relationship("AudioFile", back_populates="bbc_enhanced", cascade="all, delete")

    def __repr__(self):
        return f"<BBCArticleEnhanced(id={self.id}, title={self.title_en})>"


class HackerNewsEnhanced(Base):
    __tablename__ = "hnews_articles_enhanced"

    id = Column(Integer, primary_key=True, autoincrement=True)
    hn_id = Column(Integer, nullable=False) 
    pub_date = Column(Text, nullable=True)
    rank = Column(Integer, nullable=True) 
    link = Column(Text, nullable=True) 
    title_en = Column(Text, nullable=True)
    text_en = Column(Text, nullable=True)
    score = Column(Integer, nullable=True)
    comment_count = Column(Integer, nullable=True)
    title_de = Column(Text, nullable=True)
    title_fr = Column(Text, nullable=True)
    description_en = Column(Text, nullable=True)
    description_de = Column(Text, nullable=True)
    description_fr = Column(Text, nullable=True)
    sentiment_score_en = Column(Float, nullable=True)
    create_date = Column(Text, nullable=False, server_default=func.now())
    audio_files = relationship("AudioFile", back_populates="hnews_enhanced", cascade="all, delete")

    def __repr__(self):
        return f"<HackerNewsEnhanced(id={self.id}, title={self.title_en})>"


class GoogleTrendsEnhanced(Base):
    __tablename__ = "gtrends_terms_enhanced"

    id = Column(Integer, primary_key=True, autoincrement=True)
    term_en = Column(Text, nullable=True)
    description_en = Column(Text, nullable=True)
    description_de = Column(Text, nullable=True)
    description_fr = Column(Text, nullable=True)
    sentiment_score_en = Column(Float, nullable=True)
    link = Column(Text, nullable=True)
    pub_date = Column(Text, nullable=True)
    create_date = Column(Text, nullable=False, server_default=func.now())
    audio_files = relationship("AudioFile", back_populates="gtrends_enhanced", cascade="all, delete")

    def __repr__(self):
        return f"<GoogleTrendsEnhanced(id={self.id}, term={self.term_en})>"


email_audio_association = Table(
    "email_audio_association",
    Base.metadata,
    Column("email_id", Integer, ForeignKey("emails.id"), primary_key=True),
    Column("audio_file_id", Integer, ForeignKey("audio_files.id"), primary_key=True)
)

class AudioFile(Base):
    __tablename__ = "audio_files"

    id = Column(Integer, primary_key=True, autoincrement=True)
    bbc_id = Column(Integer, ForeignKey("bbc_articles_enhanced.id"), nullable=True)
    hnews_id = Column(Integer, ForeignKey("hnews_articles_enhanced.id"), nullable=True)
    gtrends_id = Column(Integer, ForeignKey("gtrends_terms_enhanced.id"), nullable=True)
    bbc_en = Column(Text, nullable=True)
    bbc_de = Column(Text, nullable=True)
    bbc_fr = Column(Text, nullable=True)
    hnews_en = Column(Text, nullable=True)
    hnews_de = Column(Text, nullable=True)
    hnews_fr = Column(Text, nullable=True)
    gtrends_en = Column(Text, nullable=True)
    gtrends_de = Column(Text, nullable=True)
    gtrends_fr = Column(Text, nullable=True)
    audio_file_en = Column(Text, nullable=True)
    audio_file_de = Column(Text, nullable=True)
    audio_file_fr = Column(Text, nullable=True)
    create_date = Column(Text, nullable=False, server_default=func.now())
    bbc_enhanced = relationship("BBCArticleEnhanced", back_populates="audio_files", cascade="all, delete")
    hnews_enhanced = relationship("HackerNewsEnhanced", back_populates="audio_files", cascade="all, delete")
    gtrends_enhanced = relationship("GoogleTrendsEnhanced", back_populates="audio_files", cascade="all, delete")
    emails = relationship("Email", secondary=email_audio_association, back_populates="audio_files")

    def __repr__(self):
        return f"<AudioFile(id={self.id}, audio_file_en={self.audio_file_en})>"


class Subscriber(Base):
    __tablename__ = "subscribers"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(Text, nullable=False)
    lang_preference = Column(Text, nullable=False)
    email_address = Column(Text, nullable=False, unique=True)
    create_date = Column(Text, nullable=False, server_default=func.now())
    emails = relationship("Email", back_populates="subscriber")

    def __repr__(self):
        return f"<Subscriber(id={self.id}, name={self.name}, email={self.email_address})>"


class Email(Base):
    __tablename__ = "emails"

    id = Column(Integer, primary_key=True, autoincrement=True)
    subscriber_id = Column(Integer, ForeignKey("subscribers.id"), nullable=False)
    subject = Column(Text, nullable=False)
    body = Column(Text, nullable=False)
    status = Column(Text, nullable=False)
    sent_date = Column(Text, nullable=True)
    create_date = Column(Text, nullable=False, server_default=func.now())
    subscriber = relationship("Subscriber", back_populates="emails", uselist=False)
    audio_files = relationship("AudioFile", secondary=email_audio_association, back_populates="emails")
    
    def __repr__(self):
        return f"<Email(id={self.id}, subscriber_id={self.subscriber_id}, status={self.status})>"