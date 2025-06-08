# Citi Bike Data Pipeline

This project provides an end-to-end pipeline for downloading, processing, and analyzing Citi Bike trip data. It leverages Apache Airflow for orchestration, dbt for analytics engineering, and Docker for easy deployment. Data is stored in a PostgreSQL database and can be transformed and analyzed using dbt models.

## Project Structure

- `dags/` — Airflow DAGs for orchestrating the pipeline
- `data/` — Raw Citi Bike CSV data files
- `dbt/` — dbt project for analytics engineering
- `sql/` — SQL scripts (e.g., for database initialization)
- `fetch_data.py` — Script to fetch and preprocess Citi Bike data
- `dbt_runner.sh` — Helper script to run dbt commands inside Docker
- `docker-compose.yaml` — Multi-service orchestration (Airflow, Postgres, Redis, etc.)
- `Dockerfile.dbt` — Dockerfile for dbt service
- `requirements.txt` — Python dependencies

## Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/Ppius6/citi-bike-data.git
cd citi-bike-data
```

### 2. Set Up Environment Variables

Create a `.env` file in the root directory with your database credentials:

```
DB_NAME=your_database_name
DB_USER=your_database_user
DB_PASSWORD=your_database_password
DB_HOST=your_database_host
```

### 3. Build and Start the Pipeline (Docker Compose)

```bash
docker-compose up --build
```

This will start all services (Airflow, Postgres, Redis, dbt, etc.).

### 4. Run the Data Fetch Script (Optional)

If you want to fetch data manually:

```bash
python fetch_data.py
```

### 5. Access Airflow UI

Visit [http://localhost:8080](http://localhost:8080) to monitor and trigger DAGs.

## Requirements

- Docker & Docker Compose (recommended)
- Or, for manual setup: Python 3.6+, PostgreSQL, and dependencies in `requirements.txt`

## Data Processing & Analytics

- Raw data is downloaded to `data/`.
- Airflow DAGs in `dags/` automate the ETL process.
- dbt models in `dbt/` enable analytics and transformations.

## Troubleshooting

- **ModuleNotFoundError**: Ensure all dependencies are installed and the virtual environment is activated.
- **Database Connection Issues**: Check your `.env` file and ensure the Postgres service is running.
- **Docker Issues**: Try `docker-compose down -v` to reset volumes if you encounter persistent errors.

## License

MIT License