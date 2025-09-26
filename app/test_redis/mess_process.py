import os
import time
import redis
import threading
from dotenv import load_dotenv

load_dotenv()

REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = os.getenv("REDIS_PORT")
REDIS_USER_NAME = os.getenv("REDIS_USER_NAME")
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")

KEY_LIST = os.getenv("KEY_LIST")
DEBOUNCE_KEY = os.getenv("DEBOUNCE_KEY")

r = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=True,
    username=REDIS_USER_NAME,
    password=REDIS_PASSWORD,
)

# --- TẮT LƯU DB ---
# r.config_set("save", "")
# r.config_set("appendonly", "no")
# if os.path.exists("dump.rdb"):
#     os.remove("dump.rdb")

r.config_set("notify-keyspace-events", "Ex")

# Đăng ký listener key-expire events
# pubsub = r.pubsub()
# pubsub.psubscribe('__keyevent@0__:expired')

def process_messages(chat_id):
    key_list = f"{KEY_LIST}:{chat_id}"
    msgs = r.lrange(key_list, 0, -1)
    if not msgs:
        return
    content = ", ".join(m for m in msgs).strip()
    print(f">> Process for {chat_id}: {content}")
    r.delete(key_list)

def listener():
    pubsub = r.pubsub()
    pubsub.psubscribe('__keyevent@0__:expired')
    for m in pubsub.listen():
        if m['type'] == 'pmessage':
            exp_key = m['data']
            if exp_key.startswith(f"{DEBOUNCE_KEY}:"):
                chat_id = exp_key.split(":", 1)[1]
                
                # from app.processor import process_messages
                process_messages(chat_id)

threading.Thread(target=listener, daemon=True).start()

def push_message(chat_id: str, content: str, debounce_ms: int = 5000):
    list_key = f"{KEY_LIST}:{chat_id}"
    debounce_key = f"{DEBOUNCE_KEY}:{chat_id}"
    r.rpush(list_key, content)
    # NX: tạo mới nếu không có, XX: set lại TTL nếu đã tồn tại
    if r.set(debounce_key, "", nx=True, px=debounce_ms):
        pass
    else:
        r.set(debounce_key, "", xx=True, px=debounce_ms)

# Ví dụ mô phỏng
if __name__ == "__main__":
    my = "user123"
    push_message(my, "Cha m Đạt")
    time.sleep(2)
    push_message(my, "Again")
    time.sleep(6)
