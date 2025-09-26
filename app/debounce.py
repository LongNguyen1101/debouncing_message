import os
from app.redis_client import r
from dotenv import load_dotenv

load_dotenv()

KEY_LIST = os.getenv("KEY_LIST")
DEBOUNCE_KEY = os.getenv("DEBOUNCE_KEY")

def push_message(chat_id: str, content: str, debounce_ms: int = 5000):
    list_key = f"{KEY_LIST}:{chat_id}"
    debounce_key = f"{DEBOUNCE_KEY}:{chat_id}"
    r.rpush(list_key, content)
    # NX: tạo mới nếu không có, XX: set lại TTL nếu đã tồn tại
    if r.set(debounce_key, "", nx=True, px=debounce_ms):
        pass
    else:
        r.set(debounce_key, "", xx=True, px=debounce_ms)
