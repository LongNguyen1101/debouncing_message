import os
from dotenv import load_dotenv

from fastapi import APIRouter
from app.models import Message
from app.debounce import push_message

load_dotenv()

DEBOUNCE_MS = int(os.getenv("DEBOUNCE_MS", "5000"))

router = APIRouter()

@router.post("/chat")
def receive_message(message: Message):
    """
    API nhận tin nhắn từ user, gom tin nhắn để debounce.
    """
    chat_id = message.chat_id
    user_input = message.user_input
    
    push_message(
        chat_id=chat_id,
        content=user_input,
        debounce_ms=DEBOUNCE_MS
    )
    
    return {"status": "queued", "chat_id": chat_id}
