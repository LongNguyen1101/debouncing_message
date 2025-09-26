from pydantic import BaseModel

class Message(BaseModel):
    chat_id: str
    user_input: str