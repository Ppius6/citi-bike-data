import os
import sys
from pathlib import Path

import pytest

# The backend is also launched as standalone modules in its Docker image.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ['PYTHON_DOTENV_DISABLED'] = '1'


@pytest.fixture(autouse=True)
def no_optional_memory(monkeypatch):
    monkeypatch.delenv('AI_MEMORY_PASSWORD', raising=False)
