import requests
from bs4 import BeautifulSoup
from datetime import datetime
from dotenv import load_dotenv
import os
from zipfile import ZipFile
import pandas as pd
from sqlalchemy import create_engine
import logging
import time
from pathlib import Path

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level = logging.INFO, format = '%(asctime)s - %(levelname)s - %(message)s')

# Define constants
URL = 'https://s3.amazonaws.com/tripdata/'
LATEST_DATE_FILE = 'latest_processed_date.txt'
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
                zip_ref.extractall(extract_to, members=[m for m in zip_ref.namelist() if not m.startswith('__MACOSX')])
            local_file_path.unlink()  # Remove the zip file after extraction
            logging.info(f"Extracted and deleted {file_name}")

# Combine the CSV files and return a single DataFrame
def combine_csv_files(directory=EXTRACT_DIR):
    csv_files = list(Path(directory).glob('*.csv'))
    
    if not csv_files:
        logging.error("No CSV files found in the directory")
        raise ValueError("No CSV files found in the directory")

    df_list = []
    for file_path in csv_files:
        logging.info(f"Reading {file_path.name}...")
        try:
            df = pd.read_csv(file_path, parse_dates=['started_at'], usecols=['started_at'])
            if not df.empty:
                df_list.append(df)
            else:
                logging.warning(f"{file_path.name} is empty. Skipping...")
        except Exception as e:
            logging.error(f"Error reading {file_path.name}: {e}")

    combined_df = pd.concat(df_list, ignore_index=True)
    logging.info(f"Combined {len(df_list)} files into a single DataFrame")
    
    return combined_df

# Store the processed data frame to Postgres database
def store_dataframe_to_postgres(df, table_name, conn_params):
    conn_url = f"postgresql+psycopg2://{conn_params['user']}:{conn_params['password']}@{conn_params['host']}/{conn_params['dbname']}"
    engine = create_engine(conn_url)

    latest_processed_date = get_latest_processed_date()
    logging.info(f"Latest processed date: {latest_processed_date}")

    df['started_at'] = pd.to_datetime(df['started_at'], errors='coerce')
    df = df.dropna(subset=['started_at'])
    
    if latest_processed_date:
        new_data = df[df['started_at'].dt.date > latest_processed_date]
    else:
        new_data = df

    if new_data.empty:
        logging.info("No new data available to process. Exiting.")
        return False

    new_data.to_sql(table_name, engine, if_exists='append', index=False)
    logging.info(f"Inserted {len(new_data)} records into table {table_name}")

    latest_date = new_data['started_at'].dt.date.max()
    update_latest_processed_date(latest_date)
    logging.info(f"Updated latest processed date to {latest_date}")

    return True

# Main function
def run_data_pipeline():
    links = get_file_links(URL, START_FROM)
    download_and_extract_files(links, URL)
    
    combined_df = combine_csv_files(EXTRACT_DIR)

    conn_params = {
        'dbname': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': os.getenv('DB_HOST'),
    }

    data_inserted = store_dataframe_to_postgres(combined_df, 'trips', conn_params)
    
    if data_inserted:
        logging.info("Data inserted successfully.")
    else:
        logging.info("No new data available to process.")
    
    # Save the combined data to data folder
    combined_df.to_csv(Path(EXTRACT_DIR) / 'combined_data.csv', index = False)

# Main execution
if __name__ == "__main__":
    run_data_pipeline()