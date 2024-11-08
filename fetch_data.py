import requests
from bs4 import BeautifulSoup
from datetime import datetime
from dotenv import load_dotenv
import os
from zipfile import ZipFile
import pandas as pd
from sqlalchemy import create_engine, types as sqlalchemy_types, MetaData, Table, Column, String, Float, DateTime, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import ProgrammingError, SQLAlchemyError
import logging
import time
from pathlib import Path

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level = logging.INFO, format = '%(asctime)s - %(levelname)s - %(message)s')

# Define constants
URL = 'https://s3.amazonaws.com/tripdata/'
START_FROM = 'JC-202102-citibike-tripdata.csv.zip'
EXTRACT_DIR = 'data'
CHUNK_SIZE = 8192
RETRIES = 2

# Get the latest processed date from a local file
def get_latest_processed_date():
    if Path(LATEST_DATE_FILE).exists():
        with open(LATEST_DATE_FILE, 'r') as file:
            date_str = file.read().strip()
            return datetime.strptime(date_str, '%Y-%m-%d').date()
    return None

# Update the latest processed date in a local file
def update_latest_processed_date(latest_date):
    with open(LATEST_DATE_FILE, 'w') as file:
        file.write(latest_date.strftime('%Y-%m-%d'))

# Scrape and return file links
def get_file_links(url, start_file):
    response = requests.get(url)
    response.raise_for_status()  # Raises an exception if the status code is not 200
    
    soup = BeautifulSoup(response.text, 'xml')
    keys = [key.text for key in soup.find_all('Key') if key.text.startswith('JC')]
    
    if start_file in keys:
        start_index = keys.index(start_file)
        return keys[start_index:]
    return []

# Download and extract the zip files
def download_and_extract_files(file_names, url, extract_to=EXTRACT_DIR):
    Path(extract_to).mkdir(parents=True, exist_ok=True)

    for file_name in file_names:
        file_url = f"{url}{file_name}"
        local_file_path = Path(extract_to) / file_name

        if local_file_path.with_suffix('').exists():
            logging.info(f"{file_name} already exists. Skipping download and extraction.")
            continue

        for attempt in range(RETRIES):
            try:
                logging.info(f"Downloading {file_name}...")
                with requests.get(file_url, stream=True) as r:
                    r.raise_for_status()
                    with open(local_file_path, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                            f.write(chunk)
                break
            except requests.exceptions.RequestException as e:
                logging.error(f"Failed to download {file_name}, attempt {attempt + 1}/{RETRIES}: {e}")
                time.sleep(2 ** attempt)
        else:
            logging.error(f"Failed to download {file_name} after {RETRIES} attempts")
            continue

        if local_file_path.suffix == '.zip':
            with ZipFile(local_file_path, 'r') as zip_ref:
                zip_ref.extractall(extract_to, members = [m for m in zip_ref.namelist() if not m.startswith('__MACOSX')])
            local_file_path.unlink()  # Remove the zip file after extraction
            logging.info(f"Extracted and deleted {file_name}")

# Combine the CSV files and return a single DataFrame
def combine_csv_files(directory = EXTRACT_DIR):
    csv_files = list(Path(directory).glob('*.csv'))
    
    if not csv_files:
        logging.error("No CSV files found in the directory")
        raise ValueError("No CSV files found in the directory")

    df_list = []
    
    for file_path in csv_files:
        logging.info(f"Reading {file_path.name}...")
        
        try:
            df = pd.read_csv(file_path, parse_dates = ['started_at'])           
            
            # Column names for each file
            logging.info(f"Columns in {file_path.name}: {df.columns.tolist()}")

            if not df.empty:
                df_list.append(df)
            else:
                logging.warning(f"{file_path.name} is empty. Skipping...")
        
        except Exception as e:
            logging.error(f"Error reading {file_path.name}: {e}")

    combined_df = pd.concat(df_list, ignore_index=True)
    logging.info(f"Combined {len(df_list)} files into a single DataFrame")
    
    # Print combined_df column names
    logging.info(f"Columns in combined DataFrame: {combined_df.columns.tolist()}")
    
    return combined_df

# Retrieve the latest processed date from the database
def get_latest_processed_date(conn_params, table_name):
    """
    Retrieves the latest processed date from the specified table in the database.

    Args:
        conn_params (dict): Database connection parameters.
        table_name (str): Name of the table to query.

    Returns:
        datetime.date or None: The latest processed date, or None if the table is empty.
    """
    
    # Create a connection to the database
    conn_url = f"postgresql+psycopg2://{conn_params['user']}:{conn_params['password']}@{conn_params['host']}/{conn_params['dbname']}"
    
    # Create an engine
    engine = create_engine(conn_url)
    metadata = MetaData()
    metadata.reflect(bind = engine)
    
    # Reflect the table
    try:
        trips_table = Table(table_name, metadata, autoload = True, autoload_with = engine)
    except Exception as e:
        logging.error(f"Error reflecting table {table_name}: {e}")
        return None
    
    # Create a session
    Session = sessionmaker(bind = engine)
    session = Session()
    
    try:
        # Query for the maximum started_at date
        latest_datetime = session.query(func.max(trips_table.c.started_at)).scalar()
        if latest_datetime:
            latest_date = latest_datetime.date()
            logging.info(f"Latest processed date retrieved from DB: {latest_date}")
            return latest_date
        else:
            logging.info("No data available in the table")
            return None
    except Exception as e:
        logging.error(f"Error querying the database: {e}")
        return None
    finally:
        session.close()
        
# Store the processed data frame to Postgres database
def store_dataframe_to_postgres(df, table_name, conn_params):
    """
    Stores the provided DataFrame into the PostgreSQL database.

    Args:
        df (pd.DataFrame): The DataFrame containing trip data.
        table_name (str): The name of the target table in the database.
        conn_params (dict): Database connection parameters.

    Returns:
        bool: True if data was inserted successfully, False otherwise.
    """
    # Create a connection to the database
    conn_url = f"postgresql+psycopg2://{conn_params['user']}:{conn_params['password']}@{conn_params['host']}/{conn_params['dbname']}"
    
    # Create SQLAlchemy engine 
    try:
        engine = create_engine(conn_url)
    except SQLAlchemyError as se:
        logging.error(f"Error creating engine: {se}")
        return False
    
    metadata = MetaData()
    
    # Define the table schema
    trips_table = Table(
        table_name,
        metadata,
        Column('ride_id', String, primary_key=True),
        Column('rideable_type', String),
        Column('started_at', DateTime),
        Column('ended_at', DateTime),
        Column('start_station_name', String),
        Column('start_station_id', String),
        Column('end_station_name', String),
        Column('end_station_id', String),
        Column('start_lat', Float),
        Column('start_lng', Float),
        Column('end_lat', Float),
        Column('end_lng', Float),
        Column('member_casual', String),
        extend_existing = True
    )
        
    # Create the table if it doesn't exist
    try:
        metadata.create_all(engine, tables = [trips_table])
        logging.info(f"Table {table_name} created successfully")
    except SQLAlchemyError as se:
        logging.info(f"Table {table_name} already exists")
        return False
    
    # Get the latest processed date
    latest_processed_date = get_latest_processed_date(conn_params, table_name)
    logging.info(f"Latest processed date: {latest_processed_date}")

    # Convert date columns to datetime without coercion to prevent NaT
    try:
        # Use the most flexible parsing approach
        df['started_at'] = pd.to_datetime(df['started_at'], format='mixed', errors='raise')
        df['ended_at'] = pd.to_datetime(df['ended_at'], format='mixed', errors='raise')
            
        # Verify no NaT values were created
        if df['started_at'].isna().any() or df['ended_at'].isna().any():
            logging.warning("Some dates were converted to NaT")
            return False     
                    
    except Exception as e:
        logging.error(f"Error converting date columns: {e}")
        return False
        
    # Filter new data based on the latest processed date
    if latest_processed_date:
        new_data = df[df['started_at'].dt.date > latest_processed_date]
    else:
        new_data = df

    if new_data.empty:
        logging.info("No new data available to process. Exiting.")
        return False
    
    logging.info(f"Number of new records to insert: {len(new_data)}")
    
    # Specify data types for each column
    dtype_map = {
        'ride_id': sqlalchemy_types.String,
        'rideable_type': sqlalchemy_types.String,
        'started_at': sqlalchemy_types.DateTime,
        'ended_at': sqlalchemy_types.DateTime,
        'start_station_name': sqlalchemy_types.String,
        'start_station_id': sqlalchemy_types.String,
        'end_station_name': sqlalchemy_types.String,
        'end_station_id': sqlalchemy_types.String,
        'start_lat': sqlalchemy_types.Float,
        'start_lng': sqlalchemy_types.Float,
        'end_lat': sqlalchemy_types.Float,
        'end_lng': sqlalchemy_types.Float,
        'member_casual': sqlalchemy_types.String,  
    }
    
    # Insert data into the database with conflict handling
    try:
        new_data.to_sql (
            table_name,
            engine,
            if_exists = 'append',
            index = False,
            dtype = dtype_map,
            method = 'multi' # Improved performance for multiple inserts
        )
        logging.info(f"Inserted {len(new_data)} records into table {table_name}")
    except Exception as e:
        logging.error(f"Error inserting data into the database: {e}")
        return False
    
    return True

# Main function
def run_data_pipeline():
    
    # Scrape file links starting from the specified file
    links = get_file_links(URL, START_FROM)
    if not links:
        logging.info("No new files found to process. Exiting...")
        return
    
    # Download and extract the files
    download_and_extract_files(links, URL)
    
    # Combine the CSV files into a single DataFrame
    combined_df = combine_csv_files(EXTRACT_DIR)
    
    # Define connection parameters
    conn_params = {
        'dbname': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': os.getenv('DB_HOST'),
    }
    
    # Store the combined DataFrame to the Postgres database
    data_inserted = store_dataframe_to_postgres(combined_df, 'trips', conn_params)
    
    if data_inserted:
        logging.info("Data inserted successfully.")
    else:
        logging.info("No new data available to process.")
    
    # Save the combined data to data folder
    combined_df.to_csv(Path(EXTRACT_DIR) / 'CombinedData.csv', index = False)

# Main execution
if __name__ == "__main__":
    run_data_pipeline()