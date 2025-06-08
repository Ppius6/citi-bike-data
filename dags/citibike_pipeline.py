import logging
import os
import zipfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text


class CitiBikePipeline:
    def __init__(self, config):
        self.config = config
        self.setup_logging()
        self.engine = self.create_db_engine()

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
        )
        self.logger = logging.getLogger("CitiBikePipeline")

    def create_db_engine(self):
        conn_str = (
            f"postgresql+psycopg2://{self.config['db_user']}:{self.config['db_password']}"
            f"@{self.config['db_host']}:{self.config['db_port']}/{self.config['db_name']}"
        )
        return create_engine(conn_str, pool_size=5, max_overflow=10)

    def get_file_links(self):
        """Get list of available files from S3"""
        try:
            response = requests.get(self.config["base_url"], timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "xml")
            keys = [
                key.text
                for key in soup.find_all("Key")
                if key.text.startswith("JC") and key.text.endswith(".zip")
            ]

            return sorted(keys)

        except Exception as e:
            self.logger.error(f"Error fetching file links: {e}")
            return []

    def download_and_extract(self, file_name):
        """Download and extract a single file"""
        data_dir = Path(self.config["extract_dir"])
        data_dir.mkdir(exist_ok=True)

        zip_path = data_dir / file_name
        csv_name = file_name.replace(".zip", ".csv")
        csv_path = data_dir / csv_name

        # Skip if already exists
        if csv_path.exists():
            self.logger.info(f"File already exists: {csv_name}")
            return csv_path

        try:
            # Download
            url = f"{self.config['base_url']}/{file_name}"
            self.logger.info(f"Downloading {file_name}")

            response = requests.get(url, stream=True, timeout=30)
            response.raise_for_status()

            with open(zip_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            # Extract
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                # Get the actual CSV filename from the archive
                csv_files = [
                    f
                    for f in zip_ref.namelist()
                    if f.endswith(".csv") and not f.startswith("__MACOSX")
                ]
                if csv_files:
                    actual_csv_name = csv_files[0]
                    zip_ref.extract(actual_csv_name, data_dir)
                    # Rename to expected name if different
                    extracted_path = data_dir / actual_csv_name
                    if actual_csv_name != csv_name:
                        extracted_path.rename(data_dir / csv_name)
                else:
                    raise Exception("No CSV file found in archive")

            # Remove zip file
            zip_path.unlink()

            self.logger.info(f"Extracted: {csv_name}")
            return csv_path

        except Exception as e:
            self.logger.error(f"Error processing {file_name}: {e}")
            return None

    def load_csv_to_db(self, csv_path):
        """Load CSV data to database"""
        try:
            df = pd.read_csv(csv_path)

            # Data cleaning
            df = df.dropna(subset=["ride_id", "started_at", "ended_at"])
            df["started_at"] = pd.to_datetime(df["started_at"])
            df["ended_at"] = pd.to_datetime(df["ended_at"])

            # Remove duplicates
            df = df.drop_duplicates(subset=["ride_id"])

            # Load to database
            df.to_sql(
                "trips",
                self.engine,
                schema="raw",
                if_exists="append",
                index=False,
                method="multi",
            )

            self.logger.info(f"Loaded {len(df)} records from {csv_path.name}")
            return len(df)

        except Exception as e:
            self.logger.error(f"Error loading {csv_path}: {e}")
            return 0

    def run(self):
        """Run the complete pipeline"""
        self.logger.info("Starting Citi Bike pipeline")

        # Get available files
        files = self.get_file_links()
        if not files:
            self.logger.error("No files found")
            return False

        self.logger.info(f"Found {len(files)} files to process")

        total_records = 0
        for file_name in files:
            csv_path = self.download_and_extract(file_name)
            if csv_path:
                records = self.load_csv_to_db(csv_path)
                total_records += records

        self.logger.info(f"Pipeline complete. Total records loaded: {total_records}")
        return True


if __name__ == "__main__":
    from config import config

    pipeline = CitiBikePipeline(config)
    pipeline.run()
