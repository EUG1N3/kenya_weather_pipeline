import os
from dotenv import load_dotenv
from datetime import date

load_dotenv()

GEO_URL = os.getenv("GEO_URL")
HISTORY_URL = os.getenv("HISTORY_URL")


DB_NAME=os.getenv("DB_NAME")
DB_HOST=os.getenv("DB_HOST")
DB_PORT=os.getenv("DB_PORT")
DB_USER=os.getenv("DB_USER")
DB_PASSWORD=os.getenv("DB_PASSWORD")
SSL_MODE=os.getenv("SSL_MODE")

# pipeline settings

TOWNS = ["Nairobi", "Mombasa", "Kisumu", "Nakuru", "Eldoret"]

COUNTRY_CODE = "KE"
TIMEZONE = "Africa/Nairobi"
START_DATE = date(2025, 1, 1)
DELAY_DAYS = 5

DAILY_VARIABLES = ["temperature_2m_max",
    "temperature_2m_min",
    "precipitation_sum",
    "rain_sum",
    "wind_speed_10m_max"]

FIRST_TABLE = "dim_city"
SECOND_TABLE = "dim_date"
THIRD_TABLE = "fact_daily_weather"
FOURTH_TABLE = "rejected_records"
FIFTH_TABLE = "etl_watermark"
SIXTH_TABLE = "etl_run_log"

REQUEST_TIMEOUT = 30.0