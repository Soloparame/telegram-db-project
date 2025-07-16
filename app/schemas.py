# app/schemas.py
from pydantic import BaseModel
from typing import List, Optional

class Message(BaseModel):
    message_id: int
    channel: str
    sent_at: str
    message_length: int
    has_image: bool
    text: Optional[str]

class Detection(BaseModel):
    message_id: int
    detected_object_class: str
    confidence_score: float

class ChannelActivity(BaseModel):
    date: str
    message_count: int

class TopProduct(BaseModel):
    product: str
    count: int
