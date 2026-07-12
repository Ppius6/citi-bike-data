from sentence_transformers import SentenceTransformer

# Initialize the model globally so it stays in memory after the first load.
# This downloads the model to the huggingface cache on first run.
model = SentenceTransformer('all-MiniLM-L6-v2')

def embed_text(text: str) -> list[float]:
    """Generates a 384-dimensional embedding vector for the given text."""
    # encode() returns a numpy array, we convert it to a standard Python list
    return model.encode(text).tolist()
