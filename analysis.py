import csv

# List available data files
import requests
from bs4 import BeautifulSoup

url = 'https://s3.amazonaws.com/tripdata/'

def get_zip_files_urls(url):
    try: 
        response = requests.get(url)
        response.raise_for_status() # Raises HTTPError if the HTTP request returned an unsuccessful status code
        soup = BeautifulSoup(response.text, 'html.parser')
        
        return [a['href'] for a in soup.find_all('a', href = lambda href: href and 'tripdata.zip' in href)]
    
    except Exception as e:
        print(f"Error fetching the index page: {e}")
        return []
        
# Download and extract the CSV files from ZIP archives

import os
from io import BytesIO
from zipfile import ZipFile

def download_and_extract_zip(url, extract_to):
    try:
        response= requests.get(url)
        response.raise_for_status()
    
        with ZipFile(BytesIO(response.content)) as the_zip:
            the_zip.extractall(extract_to)
            print(f"Extracted {url} to {extract_to}")
    except Exception as e:
        print(f"Error downloading and extracting the ZIP file: {e}")
        
# Insert data into the database
import psycopg2

# Database connection
conn_params = {
    'dbname': 'citi-bike-trips',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
}

def insert_csv_to_db(csv_file_path):
    try: 
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
    
        with open(csv_file_path, newline = '') as csvfile:
            reader = csv.DictReader(csvfile)
        
            for row in reader:
                cur.execute(
                    """ 
                    INSERT INTO rides (ride_id, rideable_type, started_at, ended_at, 
                                    start_station_name, start_station_id, end_station_name, 
                                    end_station_id, start_latitude, start_longitude, 
                                    end_latitude, end_longitude, member_casual)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, 
                    (row['ride_id'], row['rideable_type'], row['started_at'], row['ended_at'],
                    row['start_station_name'], row['start_station_id'], row['end_station_name'],
                    row['end_station_id'], row['start_lat'], row['start_lng'], row['end_lat'],
                    row['end_lng'], row['member_casual'])
                )
            
            conn.commit()
            print(f"Inserted {csv_file_path} into the database")
    except Exception as e:
        print(f"Error inserting CSV into the database: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()
    
# Main function
def main():
    zip_urls = get_zip_files_urls(url)
    if not zip_urls:
        print("No ZIP files found")
        return
    
    extract_to = 'extracted csv files'
    
    if not os.path.exists(extract_to):
        os.makedirs(extract_to)
        
    for zip_url in zip_urls:
        print(f'Downloading and extracting {zip_url}...')
        download_and_extract_zip(zip_url, extract_to)
        
        # Assuming each ZIP contains a single CSV file
        for filename in os.listdir(extract_to):
            if filename.endswith('.csv'):
                csv_file_path = os.path.join(extract_to, filename)
                print(f"Processing file {csv_file_path}...")
                insert_csv_to_db(csv_file_path)
                os.remove(csv_file_path)  # Clean up extracted CSV
                
        print(f"Finished processing {zip_url}")

        os.remove(os.path.join(extract_to, zip_url.split('/')[-1]))

if __name__ == '__main__':
    main()