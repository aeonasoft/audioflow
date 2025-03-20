from pydantic import BaseModel, EmailStr
from typing import Optional, List


class BBCArticleEnhancedSchema(BaseModel):
    id: Optional[int]
    link: Optional[str]
    title_en: Optional[str]
    description_en: Optional[str]
    pub_date: Optional[str]
    media_url: Optional[str]
    title_de: Optional[str]
    title_fr: Optional[str]
    description_de: Optional[str]
    description_fr: Optional[str]
    sentiment_score_en: Optional[float]
    create_date: Optional[str]
    audio_files: Optional[List["AudioFileSchema"]]

    class Config:
        from_attributes = True


class HackerNewsEnhancedSchema(BaseModel):
    id: Optional[int]
    hn_id: Optional[int]
    pub_date: Optional[str]
    rank: Optional[int]
    link: Optional[str]
    title_en: Optional[str]
    text_en: Optional[str]
    title_de: Optional[str]
    title_fr: Optional[str]
    description_en: Optional[str]
    description_de: Optional[str]
    description_fr: Optional[str]
    score: Optional[int]
    comment_count: Optional[int]
    sentiment_score_en: Optional[float]
    create_date: Optional[str]
    audio_files: Optional[List["AudioFileSchema"]]

    class Config:
        from_attributes = True


class GoogleTrendsEnhancedSchema(BaseModel):
    id: Optional[int]
    term_en: Optional[str]
    description_en: Optional[str]
    description_de: Optional[str]
    description_fr: Optional[str]
    sentiment_score_en: Optional[float]
    link: Optional[str]
    pub_date: Optional[str]
    create_date: Optional[str]
    audio_files: Optional[List["AudioFileSchema"]]

    class Config:
        from_attributes = True


class AudioFileSchema(BaseModel):
    id: Optional[int]
    bbc_id: Optional[int]
    hnews_id: Optional[int]
    gtrends_id: Optional[int]
    bbc_en: Optional[str]
    bbc_de: Optional[str]
    bbc_fr: Optional[str]
    hnews_en: Optional[str]
    hnews_de: Optional[str]
    hnews_fr: Optional[str]
    gtrends_en: Optional[str]
    gtrends_de: Optional[str]
    gtrends_fr: Optional[str]
    audio_file_en: Optional[str]
    audio_file_de: Optional[str]
    audio_file_fr: Optional[str]
    create_date: Optional[str]
    bbc_enhanced: Optional[BBCArticleEnhancedSchema] # 1:m
    hnews_enhanced: Optional[HackerNewsEnhancedSchema] # 1:m
    gtrends_enhanced: Optional[GoogleTrendsEnhancedSchema] # 1:m
    emails: Optional[List["EmailSchema"]] # m:m

    class Config:
        from_attributes = True


class SubscriberSchema(BaseModel):
    id: Optional[int] = None
    name: str
    lang_preference: str
    email_address: EmailStr # str
    create_date: Optional[str] = None
    emails: Optional[List["EmailSchema"]] = None # 1:m

    class Config:
        from_attributes = True


class EmailSchema(BaseModel):
    id: Optional[int]
    subscriber_id: int
    subject: str
    body: str
    status: str
    sent_date: Optional[str]
    create_date: Optional[str]
    subscriber: Optional[SubscriberSchema] # 1:m
    audio_files: Optional[List[AudioFileSchema]] # m:m
    
    class Config:
        from_attributes = True        