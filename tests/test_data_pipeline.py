import os
import tempfile
import pandas as pd
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock
from fetch_data import DataPipeline

# A fixture to provide a test configuration
@pytest.fixture
def test_config(tmp_path):
    return {
        "base_url": 'http://example.com',
        "start_from": 'test.zip',
        "extract_dir": str(tmp_path/"data"),
        "chunk_size": 1024,
        "max_retries": 1,
        "request_timeout": 5,
        "max_workers": 1,
        "db_chunk_size": 2,
        "table_name": 'trips',
        "output_csv": 'combined_trips.csv',
        
    }