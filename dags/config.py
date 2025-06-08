config = {
    # Data source configuration
    "base_url": "https://s3.amazonaws.com/tripdata",
    "start_from": "JC-202102-citibike-tripdata.csv.zip",
    # File processing configuration
    "extract_dir": "/opt/airflow/data",
    "chunk_size": 8192,
    "max_retries": 3,
    "request_timeout": 30,
    "max_workers": 2,
    # Database configuration
    "db_chunk_size": 1000,
    "table_name": "raw.trips",
    "db_name": "citibike",
    "db_user": "admin",
    "db_password": "password",
    "db_host": "citibike-postgres",
    "db_port": 5432,
    # Pipeline behavior
    "cleanup_temp_files": True,
    "batch_size": 10,
}
