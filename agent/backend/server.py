import os

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", ".env"))

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from agent import run_bike_agent

app = FastAPI(title="Citi Bike Chat Agent")

# Permissive CORS for local dev (`npm run dev`); in docker-compose, nginx proxies
# /api/* to this service same-origin, so CORS doesn't come into play there.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class MessageDict(BaseModel):
    role: str
    content: str


class ChatRequest(BaseModel):
    message: str
    history: list[MessageDict] = []

class ChatResponse(BaseModel):
    answer: str
    queries: list[str]


@app.get("/api/health")
def health() -> dict:
    return {"status": "ok"}


@app.post("/api/chat", response_model=ChatResponse)
def chat(request: ChatRequest) -> ChatResponse:
    history_dicts = [{"role": msg.role, "content": msg.content} for msg in request.history]
    result = run_bike_agent(request.message, history=history_dicts)
    return ChatResponse(answer=result["answer"], queries=result["queries"])
