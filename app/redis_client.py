import os
import redis
import asyncio
from threading import Thread
from dotenv import load_dotenv

load_dotenv()

REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = os.getenv("REDIS_PORT")
REDIS_USER_NAME = os.getenv("REDIS_USER_NAME")
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")
DEBOUNCE_KEY = os.getenv("DEBOUNCE_KEY")

r = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=True,
    username=REDIS_USER_NAME,
    password=REDIS_PASSWORD,
)

# disable persistence
# For testing only
# r.config_set("save", "")
# r.config_set("appendonly", "no")
# if os.path.exists("dump.rdb"):
#     os.remove("dump.rdb")

# enable key expiration events
r.config_set("notify-keyspace-events", "Ex")

def listener():
    pubsub = r.pubsub()
    pubsub.psubscribe('__keyevent@0__:expired')
    for m in pubsub.listen():
        if m['type'] == 'pmessage':
            exp_key = m['data']
            if exp_key.startswith(f"{DEBOUNCE_KEY}:"):
                chat_id = exp_key.split(":", 1)[1]
                
                from app.processor import process_messages
                process_messages(chat_id)

# def _run_listener_sync():
#     asyncio.run(listener())

# khởi listener ở bg thread
Thread(target=listener, daemon=True).start()
