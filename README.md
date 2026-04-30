# Citi Bike Data Pipeline

This project provides an automated data pipeline for downloading, processing, and storing Citi Bike trip data. It supports two approaches:

1. **Lightweight pipeline** (this branch): Downloads CSV files, processes them, and stores data in PostgreSQL and/or MinIO object storage using a modular Python package.
2. **Airflow-based pipeline** (see `dags/`): Orchestrates the ETL process using Apache Airflow, dbt for transformations, and Docker Compose for deployment.

## Project Structure

- `citibike/` — Core Python package for data ingestion, processing, and storage
- `dags/` — Airflow DAGs for orchestrating the pipeline
- `sql/` — SQL scripts for database initialization
- `tests/` — Unit tests
- `Dockerfile` — Container image for the pipeline
- `docker-compose.yml` — Services: MinIO, PostgreSQL
- `requirements.txt` — Python dependencies

## Table of Contents

- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Usage](#usage)
- [Environment Variables](#environment-variables)
- [Database Configuration](#database-configuration)
- [Troubleshooting](#troubleshooting)
- [License](#license)

## Features

- **Data Ingestion**: Downloads CSV files from the Citi Bike S3 bucket.
- **Data Processing**: Combines multiple CSV files into a single DataFrame.
- **Storage**: Stores processed data in PostgreSQL and/or MinIO (S3-compatible object storage).
- **Logging**: Comprehensive logging for tracking progress and errors.
- **Containerized**: Docker and Docker Compose support for easy deployment.
- **Tested**: Unit tests for ingestion and storage components.

## Requirements

- Python 3.8 or later
- PostgreSQL database
- MinIO (optional, for object storage)
- Docker & Docker Compose (recommended)
- Python packages listed in `requirements.txt`

## Installation

1. **Clone the Repository**

   ```bash
   git clone https://github.com/Ppius6/citi-bike-data.git
   cd citi-bike-data
   ```

2. **Create and Activate a Virtual Environment**

   ```bash
   python -m venv venv
   source venv/bin/activate  # On Unix/macOS
   venv\Scripts\activate     # On Windows
   ```

3. **Install Dependencies**

   ```bash
   pip install -r requirements.txt
   ```

4. **Set Up Environment Variables**

   Create a `.env` file in the root directory:

   ```
   DB_NAME=your_database_name
   DB_USER=your_database_user
   DB_PASSWORD=your_database_password
   DB_HOST=your_database_host
   ```

## Usage

### Run with Docker Compose

```bash
docker-compose up --build
```

This starts MinIO and PostgreSQL services.

### Run the Pipeline Manually

```bash
python -m citibike.pipeline
```

### Run Tests

```bash
pytest tests/
```

## Environment Variables

- `DB_NAME`: Name of the PostgreSQL database.
- `DB_USER`: Username for the PostgreSQL database.
- `DB_PASSWORD`: Password for the PostgreSQL database.
- `DB_HOST`: Hostname of the PostgreSQL database.

## Troubleshooting

- **ModuleNotFoundError**: Ensure all dependencies are installed and the virtual environment is activated.
- **Database Connection Issues**: Verify the database credentials and ensure the PostgreSQL server is running.
- **Docker Issues**: Try `docker-compose down -v` to reset volumes if you encounter persistent errors.

## License

MIT License
