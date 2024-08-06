# Citi Bike Data Pipeline

This project is designed to scrape, process, and store Citi Bike trip data. It downloads CSV files, combines them, and stores the processed data in a PostgreSQL database. The project utilizes several Python libraries for data handling, web scraping, and database interactions.

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

- **Data Scraping**: Downloads CSV files from a specified URL.
- **Data Processing**: Combines multiple CSV files into a single DataFrame.
- **Database Storage**: Stores processed data into a PostgreSQL database.
- **Logging**: Comprehensive logging for tracking the progress and issues.
- **Environment Variable Management**: Uses `.env` file to manage sensitive information like database credentials.

## Requirements

- Python 3.6 or later
- PostgreSQL database
- The following Python packages:
  - `requests`
  - `beautifulsoup4`
  - `python-dotenv`
  - `pandas`
  - `sqlalchemy`
  - `lxml`

## Installation

1. **Clone the Repository**

   ```
   git clone https://github.com/Ppius6/citi-bike-data.git

   cd citi-bike-data
   ```

2. **Create and Activate a Virtual Environment**

   ```
   python -m venv myenv
   myenv\Scripts\activate  # On Windows
   source myenv/bin/activate  # On Unix or MacOS
   ```

3. **Install Dependencies**

    ```
    pip install -r requirements.txt
    ```

4. **Set Up Environment Variables**
    
    Create a .env file in the root directory with the following content:

    ```
    DB_NAME=your_database_name
    DB_USER=your_database_user
    DB_PASSWORD=your_database_password
    DB_HOST=your_database_host
    ```

## Usage
To run the data pipeline, execute the fetch_data.py script:

```
python fetch_data.py
```

This script will:

- Download the latest data files from the specified URL.
- Extract and combine the CSV files into a single DataFrame.
- Insert the data into the specified PostgreSQL database.

## Environment Variables
The .env file should contain the following variables:

- DB_NAME: Name of the PostgreSQL database.
- DB_USER: Username for the PostgreSQL database.
- DB_PASSWORD: Password for the PostgreSQL database.
- DB_HOST: Hostname of the PostgreSQL database.

These variables are used to establish a connection to the PostgreSQL database.

## Database Configuration
Ensure that the PostgreSQL database is properly set up and accessible. The database credentials should match those specified in the .env file.

## Troubleshooting

- ModuleNotFoundError: Ensure all dependencies are installed and the virtual environment is activated.
- Database Connection Issues: Verify the database credentials and network connection. Ensure the PostgreSQL server is running and accessible.