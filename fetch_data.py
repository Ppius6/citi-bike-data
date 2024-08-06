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
load_dotenv()

# Configure logging
logging.basicConfig(level = logging.INFO, format = '%(asctime)s - %(levelname)s - %(message)s')

# Define the URL, latest date file, and the timeline to scrape from (maintain column consistency)
url = 'https://s3.amazonaws.com/tripdata/'
latest_date_file = 'latest_processed_date.txt'
start_from = 'JC-202102-citibike-tripdata.csv.zip'

# Get the latest processed date from a local file
def get_latest_processed_date():
    if os.path.exists(latest_date_file):
        with open(latest_date_file, 'r') as file:
            date_str = file.read().strip()
            return datetime.strptime(date_str, '%Y-%m-%d').date()
        
    return None

# Update the latest processed date in a local file
def update_latest_processed_date(latest_date):
    with open(latest_date_file, 'w') as file:
        file.write(latest_date.strftime('%Y-%m-%d'))

# Scrape and return file links
def get_file_links(url, start_file):
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'xml')
        keys = [key.text for key in soup.find_all('Key') if key.text.startswith('JC')]
        start_index = keys.index(start_file) if start_file in keys else None
        
        return keys[start_index:] if start_index is not None else []
    else:
        raise ConnectionError(f"Failed to get file links, status code: {response.status_code}")

# Download and extract the zip files
def download_and_extract_files(file_names, url, extract_to = 'data'):
    if not os.path.exists(extract_to):
        os.makedirs(extract_to)
        
    for file_name in file_names:
        file_url = f"{url}{file_name}"
        local_file_path = os.path.join(extract_to, file_name)
        
        if not os.path.exists(local_file_path.replace('.zip', '')):
            retries = 2
            
            for attempt in range(retries):
                try:
                    logging.info(f"Downloading {file_name}...")
                    with requests.get(file_url, stream=True) as r:
                        r.raise_for_status()
                        with open(local_file_path, 'wb') as f:
                            for chunk in r.iter_content(chunk_size=8192):
                                f.write(chunk)
                    break
                
                except requests.exceptions.RequestException as e:
                    logging.error(f"Failed to download {file_name}, attempt {attempt + 1}/{retries}: {e}")
                    time.sleep(2 ** attempt)
            else:
                logging.error(f"Failed to download {file_name} after {retries} attempts")
                continue
            
            if file_name.endswith('.zip'):
                with ZipFile(local_file_path, 'r') as zip_ref:
                    for file_info in zip_ref.infolist():
                        if not file_info.filename.startswith('__MACOSX'):
                            zip_ref.extract(file_info, extract_to)
                    
                os.remove(local_file_path)
                logging.info(f"Extracted and deleted {file_name}")
        else:
            logging.info(f"{file_name} already exists. Skipping download and extraction.")

# Combine the CSV files and return a single DataFrame
def combine_csv_files(directory = 'data'):
    df_list = []
    for file_name in os.listdir(directory):
        if file_name.endswith('.csv'):
            file_path = os.path.join(directory, file_name)
            
            logging.info(f"Reading {file_name}...")
            
            try:
                df = pd.read_csv(file_path, parse_dates=['started_at'])
            except Exception as e:
                logging.error(f"Error reading {file_name}: {e}")
                
                continue
            
            if df.empty:
                logging.warning(f"{file_name} is empty. Skipping...")
                
                continue
            
            df_list.append(df)
    
    if not df_list:
        logging.error("No CSV files found in the directory")
        raise ValueError("No CSV files found in the directory")
          
    combined_df = pd.concat(df_list, ignore_index = True)
    logging.info(f"Combined {len(df_list)} files into a single DataFrame")
    
    return combined_df

# Store the processed data frame to Postgres database
def store_dataframe_to_postgres(df, table_name, conn_params):
    try:
        conn_url = f"postgresql+psycopg2://{conn_params['user']}:{conn_params['password']}@{conn_params['host']}/{conn_params['dbname']}"
        engine = create_engine(conn_url)
        
        logging.info(f"Created SQLAlchemy engine: {engine}")
        
    except Exception as e:
        logging.error(f"Error creating SQLAlchemy engine: {e}")
        return False

    try:
        latest_processed_date = get_latest_processed_date()
        logging.info(f"Latest processed date: {latest_processed_date}")

        if latest_processed_date:
            df['started_at'] = pd.to_datetime(df['started_at'], errors = 'coerce')
            df = df.dropna(subset = ['started_at'])
            new_data = df[df['started_at'].dt.date > latest_processed_date]
            
            if new_data.empty:
                logging.info("No new data available to process. Exiting.")
                return False
        else:
            df['started_at'] = pd.to_datetime(df['started_at'], errors = 'coerce')
            df = df.dropna(subset = ['started_at'])
            new_data = df
            
            logging.info("Processing all data as no latest processed date found.")

        if not new_data.empty:
            table_exists = engine.dialect.has_table(engine.connect(), table_name)
            
            if table_exists:
                new_data.to_sql(table_name, engine, if_exists = 'append', index = False)
                logging.info(f"Appended {len(new_data)} records to table {table_name}")
            else:
                new_data.to_sql(table_name, engine, if_exists = 'replace', index = False)
                logging.info(f"Inserted {len(new_data)} records into new table {table_name}")

            latest_date = new_data['started_at'].dt.date.max()
            update_latest_processed_date(latest_date)
            
            logging.info(f"Updated latest processed date to {latest_date}")
        return True
    
    except Exception as e:
        logging.error(f"Error in data processing or insertion: {e}")
        return False

# Main function
def run_data_pipeline():
    combined_csv_directory = 'data'
    links = get_file_links(url, start_from)
    download_and_extract_files(links, url)
    combined_df = combine_csv_files(combined_csv_directory)

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
    logging.info("Data pipeline run complete.")
    
    # Save the combined data to data folder
    combined_df.to_csv('data/combined_data.csv', index = False)

# Main execution
if __name__ == "__main__":
    run_data_pipeline()