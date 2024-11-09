import requests
from bs4 import BeautifulSoup
from datetime import datetime
import os
from zipfile import ZipFile
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, String, Float, DateTime, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
import logging
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional, Dict, Any, Union
import pytz
from dateutil.parser import parse
import numpy as np

class DataPipeline:
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the data pipeline with configuration parameters.
        
        Args:
            config (dict): Configuration dictionary containing all necessary parameters.        
        """
        self.config = config
        self.setup_logging()
        self.setup_paths()
        self.engine = self.create_db_engine()
        
    def setup_logging(self) -> None:
        """
        Configure logging with detailed formatting.
        """
        logging.basicConfig(
            level = logging.INFO,
            format = '%(asctime)s - %(levelname)s - [%(name)s] - %(message)s',
            handlers = [
                logging.FileHandler('pipeline.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger('DataPipeline')
        
    def setup_paths(self) -> None:
        """
        Create necessary directories for storing data.
        """
        self.data_dir = Path(self.config['extract_dir'])
        self.data_dir.mkdir(parents = True, exist_ok = True)
        
    def create_db_engine(self):
        """
        Create SQLAlchemy database engine with connection pooling.
        """
        conn_str = (
            f"postgresql+psycopg2://{self.config['db_user']}:{self.config['db_password']}"
            f"@{self.config['db_host']}/{self.config['db_name']}"
        )
        
        return create_engine (
            conn_str,
            pool_size = 5,
            max_overflow = 10,
            pool_timeout = 30,
            pool_recycle = 1800
        )
        
    def get_file_links(self) -> List[str]:
        """
        Fetch and parse the file links with retry logic and validation.
            
        Returns:
            List[str]: List of URLs to data files.
        """
        for attempt in range(self.config['max_retries']):
            try:
                response = requests.get(
                    self.config['base_url'],
                    timeout = self.config['request_timeout']
                )
                response.raise_for_status()
                
                soup = BeautifulSoup(response.text, 'xml')
                keys = [
                    key.text for key in soup.find_all('Key')
                    if key.text.startswith('JC') and key.text.endswith('.zip')
                ]
                
                if self.config['start_from'] in keys:
                    start_index = keys.index(self.config['start_from'])
                    return keys[start_index:]
                return []
            
            except Exception as e:
                self.logger.error(f"Attempt {attempt + 1} failed: {str(e)}")
                if attempt < self.config['max_retries'] - 1:
                    time.sleep(2 ** attempt)
                else:
                    raise
            
    def download_file(self, file_name: str) -> Optional[Path]:
        """
        Download a single file with proper error handling and validation.
        
        Args:
            file_name (str): Name of the file to download.
            
        Returns:
            Optional[Path]: Path to the downloaded file if successful, None otherwise.
        """
        zip_path = self.data_dir / file_name
        csv_path = zip_path.with_suffix('')
        
        if csv_path.exists():
            self.logger.info(f"CSV already exists: {csv_path.name}")
            return None # No need to download if CSV already exists
        
        if zip_path.exists():
            self.logger.info(f"Zip file already exists: {zip_path.name}")
            return zip_path # Return the existing zip file path
               
        try:
            url = f"{self.config['base_url']}/{file_name}"
            with requests.get(url, stream = True, timeout = self.config['request_timeout']) as r:
                r.raise_for_status()
                with open(zip_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size = self.config['chunk_size']):
                        f.write(chunk)
            self.logger.info(f"Downloaded Zip file: {zip_path.name}")
            return zip_path
        
        except Exception as e:
            self.logger.error(f"Error downloading {file_name}: {str(e)}")
            return None
        
    def process_downloaded_file(self, file_path: Path) -> Optional[pd.DataFrame]:
        """
        Process a single downloaded file (extract if zip and read CSV).
        
        Args:
            file_path (Path): Path to the downloaded file.
            
        Returns:
            Optional[pd.DataFrame]: Processed DataFrame if successful, None if processing failed.
        """
        if file_path is None:
            # No file to process
            return None 
        try:
            if file_path.suffix == '.zip':
                with ZipFile(file_path, 'r') as zip_ref:
                    csv_name = next(
                        name for name in zip_ref.namelist()
                        if name.endswith('.csv') and not name.startswith('_MACOSX')
                    )
                    zip_ref.extract(csv_name, self.data_dir)
                self.logger.info(f"Extracted CSV file from ZIP: {file_path.name}")
                file_path.unlink() # Remove the zip file after extraction
                file_path = self.data_dir / csv_name
                
            return self.read_and_clean_csv(file_path)
        
        except Exception as e:
            self.logger.error(f"Error processing {file_path}: {str(e)}")
            return None
        
    def read_and_clean_csv(self, file_path: Path) -> pd.DataFrame:
        """
        Read a CSV file into a DataFrame and perform basic cleaning.
        
        Args:
            file_path (Path): Path to the CSV file.
            
        Returns:
            pd.DataFrame: Processed DataFrame.
        """
        df = pd.read_csv(file_path)
        
        # Date parsing with multiple formats
        date_columns = ['started_at', 'ended_at']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], format = 'mixed', utc = True)
                df[col] = df[col].dt.tz_convert('UTC')
                
        # Remove duplicate primary keys
        df = df.drop_duplicates(subset = ['ride_id'])
        
        return df
    
    def get_latest_processed_date(self) -> Optional[datetime]:
        """
        Get the latest processed date from the database with proper error handling.
        
        Returns:
            Optional[datetime]: Latest date already processed, None if no data is available.
        """
        try:
            metadata = MetaData()
            trips_table = Table(
                self.config['table_name'],
                metadata,
                autoload_with = self.engine
            )
            
            with self.engine.connect() as conn:
                result = conn.execute (
                    func.max(trips_table.c.started_at)
                ).scalar()
                
            return result.replace(tzinfo = pytz.UTC) if result else None
        
        except Exception as e:
            self.logger.error(f"Error fetching latest date: {str(e)}")
            return None
        
    def store_dataframe(self, df: pd.DataFrame) -> bool:
        """
        Store DataFrame to database with efficient batching and error handling.
        
        Args:
            df (pd.DataFrame): DataFrame to store
            
        Returns:
            bool: True if successful, False otherwise
        """
        
        if df.empty:
            self.logger.info("No data to store.")
            return True
        
        latest_date = self.get_latest_processed_date()
        if latest_date:
            df = df[df['started_at'] > latest_date]
            if df.empty:
                self.logger.info("No new data to store.")
                return True
            
        try:
            chunk_size = self.config['db_chunk_size']
            for i in range(0, len(df), chunk_size):
                chunk = df.iloc[i:i + chunk_size]
                chunk.to_sql(
                    self.config['table_name'],
                    self.engine,
                    if_exists = 'append',
                    index = False,
                    method = 'multi'
                )
                
            self.logger.info(f"Stored {len(df)} records.")
            return True
        
        except Exception as e:
            self.logger.error(f"Error storing data: {str(e)}")
            return False
        
    def run(self) -> bool:
        """
        Execute the complete data pipeline with parallel processing.
        
        Returns:
            bool: True if successful, False otherwise.
        """
        
        try:
            file_links = self.get_file_links()
            if not file_links:
                self.logger.info("No new files to download.")
                return True
            
            # Process files in parallel
            processed_dfs = []
            
            with ThreadPoolExecutor(max_workers = self.config['max_workers']) as executor:
                # Download files
                download_results = list(executor.map(self.download_file, file_links))
                
                # Process downloaded files
                for file_path in download_results:
                    df = self.process_downloaded_file(file_path)
                    if df is not None:
                        processed_dfs.append(df)
                        
            if not processed_dfs:
                self.logger.error("No data processed.")
                return True # Considered successful as there is no new data to process
            
            # Combine all processed DataFrames
            combined_df = pd.concat(processed_dfs, ignore_index = True)
            return self.store_dataframe(combined_df)
        
        except Exception as e:
            self.logger.error(f"Pipeline failed: {str(e)}")
            return False
        
if __name__ == '__main__':
    config = {
        'base_url': 'https://s3.amazonaws.com/tripdata',
        'start_from': 'JC-202102-citibike-tripdata.csv.zip',
        'extract_dir': 'data',
        'chunk_size': 8192,
        'max_retries': 3,
        'request_timeout': 30,
        'max_workers': 4,
        'db_chunk_size': 5000,
        'table_name': 'trips',
        'db_name': os.getenv('DB_NAME'),
        'db_user': os.getenv('DB_USER'),
        'db_password': os.getenv('DB_PASSWORD'),
        'db_host': os.getenv('DB_HOST')
    }
    
    pipeline = DataPipeline(config)
    success = pipeline.run()
    
    if success:
        logging.info("Pipeline completed successfully.")
    else:
        logging.error("Pipeline failed.")