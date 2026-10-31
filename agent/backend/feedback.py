"""Short-lived, bounded approval candidates; API clients never submit SQL to save."""
from collections import OrderedDict
from threading import Lock
from time import monotonic
from uuid import uuid4

from database import get_schema_context, save_memory

_pending = OrderedDict()
_lock = Lock()
CANDIDATE_TTL_SECONDS = 900
MAX_CANDIDATES = 512


def _expire():
    for key in list(_pending):
        if _pending[key][0] <= monotonic():
            del _pending[key]


def create_candidate(question: str, query: str, schema_version: str) -> str:
    candidate_id = str(uuid4())
    with _lock:
        _expire()
        while len(_pending) >= MAX_CANDIDATES:
            _pending.popitem(last=False)
        _pending[candidate_id] = (monotonic() + CANDIDATE_TTL_SECONDS,
                                  question, query, schema_version)
    return candidate_id


def submit_feedback(candidate_id: str, approved: bool) -> str:
    with _lock:
        _expire()
        candidate = _pending.pop(candidate_id, None)
    if candidate is None:
        return "expired"
    if not approved:
        return "discarded"
    _, question, query, version = candidate
    try:
        if get_schema_context().version != version:
            return "expired"
        if save_memory(question, query, version):
            return "saved"
    except Exception:
        pass  # Optional memory must never take the chat service down.
    with _lock:
        if len(_pending) < MAX_CANDIDATES and candidate[0] > monotonic():
            _pending[candidate_id] = candidate
    return "unavailable"
