from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

MAX_MESSAGE_CHARS = 4000
MAX_HISTORY_MESSAGES = 20
MAX_CONTEXT_CHARS = 24000


class MessageDict(BaseModel):
    model_config = ConfigDict(extra="forbid")

    role: Literal["user", "assistant"]
    content: str = Field(min_length=1, max_length=MAX_MESSAGE_CHARS)


class ChatRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    message: str = Field(min_length=1, max_length=MAX_MESSAGE_CHARS)
    history: list[MessageDict] = Field(default_factory=list, max_length=MAX_HISTORY_MESSAGES)

    @model_validator(mode="after")
    def limit_context(self):
        if len(self.message) + sum(len(msg.content) for msg in self.history) > MAX_CONTEXT_CHARS:
            raise ValueError("Conversation is too long; start a new conversation.")
        return self


class QueryResult(BaseModel):
    status: Literal["ok", "empty", "error"]
    columns: list[str] = Field(default_factory=list)
    rows: list[list[Any]] = Field(default_factory=list)
    source_tables: list[str] = Field(default_factory=list)
    truncated: bool = False
    error: str | None = None


class SQLArguments(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    query: str = Field(min_length=1, max_length=20000)
