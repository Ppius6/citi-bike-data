import logging
import os
from typing import Literal
from uuid import UUID

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", ".env"))

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, ConfigDict, Field

from agent import run_bike_agent
from contracts import ChatRequest
from feedback import submit_feedback

# Surface the agent's per-request metrics line (logged at INFO).
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

app = FastAPI(title="Citi Bike Chat Agent")

# Permissive CORS for local dev (`npm run dev`); in docker-compose, nginx proxies
# /api/* to this service same-origin, so CORS doesn't come into play there.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class ChatResponse(BaseModel):
    answer: str
    queries: list[str]
    metrics: dict[str, int | float] = Field(default_factory=dict)
    memory_candidate_id: str | None = None


class FeedbackRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    candidate_id: str
    approved: bool


class FeedbackResponse(BaseModel):
    status: Literal["saved", "discarded"]


@app.get("/api/health")
def health() -> dict:
    return {"status": "ok"}


@app.post("/api/chat", response_model=ChatResponse)
def chat(request: ChatRequest) -> ChatResponse:
    history_dicts = [msg.model_dump() for msg in request.history]
    result = run_bike_agent(request.message, history=history_dicts)
    return ChatResponse(**result)


@app.post("/api/feedback", response_model=FeedbackResponse)
def feedback(request: FeedbackRequest) -> FeedbackResponse:
    try:
        UUID(request.candidate_id)
    except ValueError:
        raise HTTPException(status_code=422, detail="Invalid feedback candidate.")
    status = submit_feedback(request.candidate_id, request.approved)
    if status == "expired":
        raise HTTPException(status_code=410, detail="This feedback candidate has expired.")
    if status == "unavailable":
        raise HTTPException(status_code=503, detail="Memory is unavailable; please retry.")
    return FeedbackResponse(status=status)
