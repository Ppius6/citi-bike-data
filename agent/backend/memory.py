from functools import lru_cache

@lru_cache(maxsize=1)
def _get_model():
    from sentence_transformers import SentenceTransformer

    # Chat requests must never download a model. Pre-cache it during setup;
    # missing models disable optional memory through the caller's error handler.
    return SentenceTransformer('all-MiniLM-L6-v2', local_files_only=True)

@lru_cache(maxsize=256)
def embed_text(text: str) -> list[float]:
    """Generates a 384-dimensional embedding vector for the given text."""
    # encode() returns a numpy array, we convert it to a standard Python list
    return _get_model().encode(text).tolist()
