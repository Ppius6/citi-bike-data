import logging
import time
from datetime import datetime, timezone, timedelta
from io import StringIO
import requests
import pandas as pd
from sqlalchemy import create_engine, text

from scripts.config.config import Config

logger = logging.getLogger(__name__)

class WeatherIngester:
    def __init__(self, config: Config):
        self.postgres = config.postgres
        self.engine = create_engine(self.postgres.url)
        self.latitude = 40.71
        self.longitude = -74.00
        self.api_url = "https://archive-api.open-meteo.com/v1/archive"
        # We need historical data. 
        # Open-Meteo's archive API goes up to a few weeks ago. For recent days, we use the forecast API, 
        # but the archive API is usually sufficient if we just use "historical weather API".
        # Actually, https://api.open-meteo.com/v1/forecast can also return past days.
        # But `historical` API is safer for deep backfill:
        # https://archive-api.open-meteo.com/v1/archive

    def _get_last_ingested_date(self) -> str:
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT MAX(observation_time) FROM bronze.weather")).scalar()
                if result:
                    # Return the next day
                    next_day = result + timedelta(days=1)
                    return next_day.strftime("%Y-%m-%d")
        except Exception as e:
            logger.warning(f"Failed to query max observation_time (might be first run): {e}")
        return "2021-02-01"

    def fetch_weather_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        params = {
            "latitude": self.latitude,
            "longitude": self.longitude,
            "start_date": start_date,
            "end_date": end_date,
            "hourly": "temperature_2m,precipitation,windspeed_10m,weathercode,is_day",
            "timezone": "UTC"
        }
        logger.info(f"Fetching weather from {start_date} to {end_date}")
        response = requests.get(self.api_url, params=params, timeout=30)
        
        if response.status_code != 200:
            logger.error(f"Open-Meteo API error: {response.text}")
            response.raise_for_status()

        data = response.json()
        
        if "hourly" not in data:
            logger.warning("No hourly data found in response.")
            return pd.DataFrame()

        hourly = data["hourly"]
        df = pd.DataFrame({
            "observation_time": pd.to_datetime(hourly["time"], utc=True),
            "temperature_c": hourly.get("temperature_2m", []),
            "precipitation_mm": hourly.get("precipitation", []),
            "wind_speed_kmh": hourly.get("windspeed_10m", []),
            "weather_code": hourly.get("weathercode", []),
            "is_daylight": hourly.get("is_day", [])
        })

        # Add ingestion timestamp
        df["_ingested_at"] = pd.Timestamp.now(tz="UTC")
        
        # Open-Meteo uses 1/0 for is_day, convert to boolean
        if "is_daylight" in df.columns:
            df["is_daylight"] = df["is_daylight"].astype(bool)

        return df

    def load_to_postgres(self, df: pd.DataFrame) -> bool:
        if df.empty:
            logger.info("Empty DataFrame, nothing to load.")
            return True

        row_count = len(df)
        csv_buffer = StringIO()
        # Drop rows where observation_time is NaT just in case
        df = df.dropna(subset=['observation_time'])
        df.to_csv(csv_buffer, index=False, header=False)
        csv_buffer.seek(0)

        columns = ", ".join(df.columns)
        copy_sql = f"COPY {self.postgres.bronze_schema}.weather ({columns}) FROM STDIN WITH (FORMAT csv)"

        raw_conn = self.engine.raw_connection()
        try:
            with raw_conn.cursor() as cursor:
                cursor.copy_expert(copy_sql, csv_buffer)
            raw_conn.commit()
            logger.info(f"Successfully loaded {row_count} rows into bronze.weather via COPY")
            return True
        except Exception as e:
            raw_conn.rollback()
            logger.error(f"Failed to load weather data: {e}")
            return False
        finally:
            raw_conn.close()

    def run(self) -> bool:
        start_date = self._get_last_ingested_date()
        # End date is 5 days ago to ensure archive API has the data 
        # (Open-Meteo archive lags by about 5 days)
        end_date = (datetime.now(timezone.utc) - timedelta(days=5)).strftime("%Y-%m-%d")

        if start_date > end_date:
            logger.info(f"Weather data is up to date (start_date {start_date} > end_date {end_date}).")
            return True

        try:
            df = self.fetch_weather_data(start_date, end_date)
            if not df.empty:
                return self.load_to_postgres(df)
            return True
        except Exception as e:
            logger.error(f"Weather ingestion failed: {e}")
            return False

def run_weather_ingest(config: Config) -> bool:
    ingester = WeatherIngester(config)
    return ingester.run()
