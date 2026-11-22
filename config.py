import os
from dotenv import load_dotenv
from datetime import date

load_dotenv()

GEO_URL = os.getenv("GEO_URL")
HISTORY_URL = os.getenv("HISTORY_URL")

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

REQUEST_TIMEOUT = 30.0