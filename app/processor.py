import os
import uuid
import httpx
import asyncio
import traceback
from dotenv import load_dotenv

from app.redis_client import r
from app.log.logger_config import setup_logging

logger = setup_logging(__name__)

load_dotenv(override=True)

CHATBOT_URL = os.getenv("CHATBOT_URL")

KEY_LIST = os.getenv("KEY_LIST")
DEBOUNCE_KEY = os.getenv("DEBOUNCE_KEY")
LOCK_KEY = os.getenv("LOCK_KEY")
    
def send_to_server(chat_id: str, user_input: str):
    payload = {
        "chat_id": chat_id,
        "user_input": user_input
    }
    
    timeout = 50
    with httpx.Client(timeout=timeout) as client:
        resp = client.post(
            str(CHATBOT_URL),
            json=payload
        )
        resp.raise_for_status()
        resp = resp.json()
        logger.info(f"Sent to server: {payload}, received status: {resp}")
        
        return resp

# async def bg_send_to_server(chat_id: str, text: str):
#     try:
#         await send_to_server(chat_id, text)
#     except Exception as e:
#         error_details = traceback.format_exc()
#         logger.error(f"Exception: {e}")
#         logger.error(f"Chi tiết lỗi: \n{error_details}")
#         raise


# Lua script release lock an toàn
RELEASE_SCRIPT = """
if redis.call("get", KEYS[1]) == ARGV[1] then
  return redis.call("del", KEYS[1])
else
  return 0
end
"""
release_lock = r.register_script(RELEASE_SCRIPT)

def acquire_lock(lock_key: str, ttl_ms: int = 6000) -> str | None:
    token = str(uuid.uuid4())
    ok = r.set(lock_key, token, nx=True, px=ttl_ms)
    return token if ok else None

def free_lock(lock_key: str, token: str) -> None:
    try:
        release_lock(keys=[lock_key], args=[token])
    except Exception as e:
        logger.error(f"[WARN] free_lock exception: {e}")

def process_messages(chat_id: str):
    lock_key = f"{LOCK_KEY}:{chat_id}"
    token = acquire_lock(lock_key)
    if not token:
        logger.info(f"Skip process_messages({chat_id}): already locked")
        return

    try:
        key_list = f"{KEY_LIST}:{chat_id}"
        msgs = r.lrange(key_list, 0, -1)
        if not msgs:
            return
        content = ", ".join(m for m in msgs).strip()
        r.delete(key_list)

        # asyncio.create_task(bg_send_to_server(chat_id=chat_id, text=content))
        send_to_server(chat_id=chat_id, user_input=content)
    finally:
        free_lock(lock_key, token)