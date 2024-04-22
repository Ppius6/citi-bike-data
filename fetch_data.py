
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import os
from zipfile import ZipFile
import pandas as pd
from sqlalchemy import create_engine
import urllib

url = 'https://s3.amazonaws.com/tripdata/'

# FUnction to scrape and return file links
def get_file_links(url, start_file):
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'xml')
        # Find all links in the response
        keys = [key.text for key in soup.find_all('Key') if key.text.startswith('JC')]
        
        # Find the index for the start file and return links from that file onward
        start_index = keys.index(start_file) if start_file in keys else None
        
        return keys[start_index:] if start_index is not None else []
    else:
        raise ConnectionError(f"Failed to get file links, status code: {response.status_code}")
        
# Function to download and extract the zip files
def download_and_extract_files(links, url, extract_to = 'data'):
    if not os.path.exists(extract_to):
        os.makedirs(extract_to)
    
    for file_name in links:
        file_url = f"{url}{file_name}"
        local_zip_path = os.path.join(extract_to, file_name)
        extracted_file_name = file_name.replace('.zip', '.csv')
        local_csv_path = os.path.join(extract_to, extracted_file_name)
        
        # Skip download and extraction if the CSV file already exists
        if os.path.isfile(local_csv_path):
            print(f"File {extracted_file_name} already exists, skipping download and extraction")
            continue
        
        print(f"Downloading {file_name}...")
        with requests.get(file_url, stream = True) as r:
            r.raise_for_status()
            
            with open(local_zip_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size = 8192):
                    f.write(chunk)
                    
        # Extract the zip file if it exists
        if file_name.endswith('.zip'):
            with ZipFile(local_zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_to)
                
            os.remove(local_zip_path)
            print(f"Extracted {extracted_file_name}")
   
# Function to combine the CSV files
def combine_csv_files(directory):
    
    csv_files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.csv')]
    df_list = [pd.read_csv(csv_file) for csv_file in csv_files]
    
    if not all(df.columns.equals(df_list[0].columns) for df in df_list):
        raise ValueError("Not all CSV files have the same columns")
    
    combined_df = pd.concat(df_list, ignore_index = True)
    
    return combined_df
        
# Function to store DataFrame to PostgreSQL
def store_dataframe_to_postgres(df, table_name, conn_params):
    
    try:
        # Encoding the password
        password = urllib.parse.quote_plus(conn_params['password'])
        
        # Create the connection url
        conn_url = f"postgresql://{conn_params['user']}:{password}@{conn_params['host']}:{conn_params['port']}/{conn_params['dbname']}"
        
        # Create the database engine
        with create_engine(conn_url).connect() as connection:
            
            df.to_sql(table_name, connection, if_exists = 'replace', index = False)
            
        print(f"Data stored in table {table_name} in database {conn_params['dbname']}")
        
    except Exception as e:
        
        print(f"An error occurred: {e}")
  
# Main execution
if __name__ == "__main__":
    
    start_from = 'JC-202102-citibike-tripdata.csv.zip'
    links = get_file_links(url, start_from)
    download_and_extract_files(links, url)
    
    combined_csv_directory = 'data'
    combined_df = combine_csv_files(combined_csv_directory)
    
    # Database connection parameters
    conn_params = {
        'dbname': 'citi-bike-trips',
        'user': 'postgres',
        'password': "Pius@21!",
        'host': 'localhost',
        'port': '5432'
        }
    
    store_dataframe_to_postgres(combined_df, 'trips', conn_params)