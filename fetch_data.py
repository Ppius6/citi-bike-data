import requests
from bs4 import BeautifulSoup
from datetime import datetime
from pytz import timezone
import os
from zipfile import ZipFile
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, String, Float, DateTime, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.postgresql import insert
import logging
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional, Dict, Any, Union, Tuple
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
        self.combined_df = None
        
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
                    raise Exception("Max retries exceeded.")
            
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
        ny_tz = timezone('America/New_York')
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], format = 'mixed', utc = True)
                df[col] = df[col].dt.tz_convert(ny_tz)
                
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
            
            ny_tz = timezone('America/New_York')   
            return result.astimezone(ny_tz) if result else None
        
        except Exception as e:
            self.logger.error(f"Error fetching latest date: {str(e)}")
            return None
        
    def get_combined_dataframe(self) -> Optional[pd.DataFrame]:
        """
        Retrieve or create a combined DataFrame from existing CSV files.
        
        Returns:
            Optional[pd.DataFrame]: Combined DataFrame if successful, None otherwise.
        """
        
        # Check if a combined DataFrame already exists
        if self.combined_df is not None:
            return self.combined_df
        
        # Load all CSV files in the data directory
        csv_files = list(self.data_dir.glob('*.csv'))
        
        if not csv_files:
            self.logger.info("No CSV files found.")
            return None
        
        try:
            # Read all CSV files into a single DataFrame
            dfs = []
            for file in csv_files:
                df = pd.read_csv(file)
                dfs.append(df)
                
            combined_df = pd.concat(dfs, ignore_index = True)
            
            # Remove duplicates
            combined_df.drop_duplicates(subset = ['ride_id'], inplace = True)
            
            # Date parsing with multiple formats
            date_columns = ['started_at', 'ended_at']
            ny_tz = timezone('America/New_York')
            for col in date_columns:
                if col in combined_df.columns:
                    combined_df[col] = pd.to_datetime(combined_df[col], format = 'mixed', utc = True)
                    combined_df[col] = combined_df[col].dt.tz_convert(ny_tz)
            
            self.logger.info(f"Combined DataFrame has {len(combined_df)} records after removing duplicates.")
            
            self.combined_df = combined_df
            
            return combined_df
        
        except Exception as e:
            self.logger.error(f"Error combining DataFrames: {str(e)}")
            return None
    
    def store_dataframe(self, df: pd.DataFrame) -> bool:
        """
        Store DataFrame to database with duplicate prevention, efficient batching and error handling.
    
        Args:
            df (pd.DataFrame): DataFrame to store
            
        Returns:
            bool: True if successful, False otherwise
        """
        
        if df.empty:
            self.logger.info("No data to store.")
            return True

        try:
            # Check for duplicates
            initial_count = len(df)
            duplicate_rides = df[df['ride_id'].duplicated(keep = False)]
            
            if not duplicate_rides.empty:
                self.logger.warning(
                    f"Found {len(duplicate_rides)} duplicate ride IDs. "
                    "Keeping only the first occurrence of each ride_id."
                )
                
                # Logging duplicate rides for reference
                if len(duplicate_rides) > 0:
                    sample_duplicates = duplicate_rides.groupby('ride_id').head(2).head(5)
                    self.logger.debug(f"Sample duplicate rides:\n{sample_duplicates}")
                    
            # Remove duplicates keeping the first occurrence
            df.drop_duplicates(subset = ['ride_id'], keep = 'first', inplace = True)
            
            # Get the latest processed date to avoid duplicates
            latest_date = self.get_latest_processed_date()
            if latest_date:
                df = df[df['started_at'] > latest_date]
                if df.empty:
                    self.logger.info("No new data to store.")
                    return True
            
            # Log the final counts
            final_count = len(df)
            removed_count = initial_count - final_count
            
            if removed_count > 0:
                self.logger.info(
                    f"Removed {removed_count} duplicate records "
                    f"({(removed_count / initial_count) * 100:.2f}% of original data)"
                )
                
            # Store the deduplicated data in chunks
            chunk_size = self.config['db_chunk_size']
            records_stored = 0

            for i in range(0, len(df), chunk_size):
                chunk = df.iloc[i:i + chunk_size]
                chunk.to_sql(
                    self.config['table_name'],
                    self.engine,
                    if_exists = 'append',
                    index = False,
                    method = 'multi'
                )
                
                records_stored += len(chunk)
                self.logger.info(
                    f"Stored {records_stored} records out of {final_count} "
                    f"Progress: {records_stored / len(df)}"
                )
                
            self.logger.info(f"Successfully stored {records_stored} unique records.")
            return True
        
        except Exception as e:
            self.logger.error(f"Error storing data: {str(e)}")
            return False
        
    def save_dataframe(self, df: pd.DataFrame) -> bool:
        """
        Save DataFrame to the data directory as a CSV file.
        
        Args:
            df (pd.DataFrame): DataFrame to save.
            
        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            file_name = self.config.get('output_csv', 'combined_trips.csv')
            file_path = self.data_dir / file_name
            df.to_csv(file_path, index = False)
            
            self.logger.info(f"Saved DataFrame to {file_path.name}")
            return True
        
        except Exception as e:
            self.logger.error(f"Error saving DataFrame: {str(e)}")
            return False
    
    def run(self) -> Tuple[bool, Optional[pd.DataFrame]]:
        """
        Execute the complete data pipeline with parallel processing.
        
        Returns:
            Tuple[bool, Optional[pd.DataFrame]]: 
                - First element is True if successful, False otherwise
                - Second element is the combined DataFrame if successful, None otherwise
        """
        try:
            file_links = self.get_file_links()
            
            # If no new files are found, try to get existing combined DataFrame
            if not file_links:
                existing_df = self.get_combined_dataframe()
                if existing_df is not None:
                    self.logger.info("Using existing DataFrame.")
                    self.save_dataframe(existing_df)
                    return True, existing_df
                else:
                    self.logger.info("No new files.")
                    return True, None
            
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
                # If no new processed files, get existing combined DataFrame
                existing_df = self.get_combined_dataframe()
                if existing_df is not None:
                    self.save_dataframe(existing_df)
                    return True, existing_df
                
                self.logger.error("No data processed.")
                return True, None # Considered successful as there is no new data to process
            
            # Combine all processed DataFrames
            combined_df = pd.concat(processed_dfs, ignore_index = True)
            
            combined_df.drop_duplicates(subset = ['ride_id'], inplace = True)
            
            self.logger.info(f"Combined DataFrame has {len(combined_df)} records after removing duplicates.")
            
            # Save the combined DataFrame to the data directory
            save_success = self.save_dataframe(combined_df)
            if not save_success:
                self.logger.error("Error saving DataFrame.")
                return False, None
            
            # Store the combined DataFrame to the database
            store_success = self.store_dataframe(combined_df)
            
            # Update the combined DataFrame if stored successfully
            if store_success:
                self.combined_df = combined_df
                            
            return store_success, combined_df
        
        except Exception as e:
            self.logger.error(f"Pipeline failed: {str(e)}")
            return False, None
    
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
        'output_csv': 'combined_trips.csv',
        'db_name': os.getenv('DB_NAME'),
        'db_user': os.getenv('DB_USER'),
        'db_password': os.getenv('DB_PASSWORD'),
        'db_host': os.getenv('DB_HOST')
    }
    
    pipeline = DataPipeline(config)
    success, df = pipeline.run()
    
    if success:
        logging.info("Pipeline completed successfully.")
        if df is not None:
            logging.info(f"DataFrame has {len(df)} records.")
            output_csv_path = Path(config['extract_dir']) / config['output_csv']
            logging.info(f"Combined CSV file saved at: {output_csv_path.resolve()}")
        else:
            logging.info("No new data to process.")
    else:
        logging.error("Pipeline failed.")