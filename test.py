
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import os
from zipfile import ZipFile
import pandas as pd
from sqlalchemy import create_engine

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
def download_and_extract_files(file_name, url, extract_to = 'data'):
    if not os.path.exists(extract_to):
        os.makedirs(extract_to)
    
    for file_name in file_name:
        file_url = f"{url}{file_name}"
        local_file_path = os.path.join(extract_to, file_name)
        
        # Download the file
        print(f"Downloading {file_name}...")
        with requests.get(file_url, stream = True) as r:
            r.raise_for_status()
            
            with open(local_file_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size = 8192):
                    f.write(chunk)
    
        
        # Extract the file, if it is a zip file
        if file_name.endswith('.zip'):
            with ZipFile(local_file_path, 'r') as zip_ref:
                zip_ref.extractall(extract_to)
        
            # Delete the zip file
            os.remove(local_file_path)
            print(f"Extracted and deleted {file_name}")
            
    else:
        
        print(f"Failed to download {file_name}, status code: {r.status_code}")
     
   
# Function to combine the CSV files
def combine_csv_files(directory):
    
    # Empty lists to hold dataframes
    df_list = []
    first_file = True
    columns_expected = None
    
    # Iterate over each file in the directory
    for file_name in os.listdir(directory):
        if file_name.endswith('.csv'):
            file_path = os.path.join(directory, file_name)
            
            # Read the CSV file
            df = pd.read_csv(file_path)
            
            # For the first file, save the column structure
            if first_file:
                columns_expected = df.columns
                first_file = False
                
            # If the columns do not match, raise an error
            elif not df.columns.equals(columns_expected):
                raise ValueError(f"Columns in {file_name} do not match the expected columns")
            
            # Append the dataframe to the list
            df_list.append(df)
            
    # Concatenate the dataframes into a single dataframe
    combined_df = pd.concat(df_list, ignore_index = True)
    return combined_df
        
# Function to store DataFrame to PostgreSQL
def store_dataframe_to_postgres(df, table_name, conn_params):
    
    # Create a connection URL
    conn_url = f"postgresql://{conn_params['user']}:{conn_params['password']}@{conn_params['host']}/{conn_params['dbname']}"
    
    # Create a SQLAlchemy engine
    engine = create_engine(conn_url)
    
    # Pandas to_sql method to store the DataFrame
    df.to_sql(table_name, engine, index = False, if_exists = 'replace')
    
    print(f"Stored DataFrame to {table_name} table in PostgreSQL database")
  
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
        'password': 'postgres',
        'host': 'localhost',
        }
    
    store_dataframe_to_postgres(combined_df, 'trips', conn_params)