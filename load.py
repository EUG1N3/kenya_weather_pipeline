import os
import sys
import json
import logging
import pandas as pd
from datetime import date, datetime, timedelta, timezone
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.exc import SQLAlchemyError

import config
from extract import main as run_extraction
from transform import run_transformations

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 1. Connection
# ---------------------------------------------------------------------------
def get_engine():
    url = URL.create(
        drivername="postgresql+psycopg2",
        database=config.DB_NAME,
        host=config.DB_HOST,
        port=config.DB_PORT,
        username=config.DB_USER,
        password=config.DB_PASSWORD,
    )
    return create_engine(url, connect_args={"sslmode": config.SSL_MODE}, pool_pre_ping=True)


# ---------------------------------------------------------------------------
# 2. DDL Statements
# ---------------------------------------------------------------------------
CREATE_DIM_CITY = f"""
CREATE TABLE IF NOT EXISTS {config.FIRST_TABLE} (
    city_id BIGINT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    county VARCHAR(50),
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    elevation DOUBLE PRECISION,
    population INTEGER,
    loaded_at TIMESTAMPTZ DEFAULT now()
);"""

CREATE_DIM_DATE = f"""
CREATE TABLE IF NOT EXISTS {config.SECOND_TABLE} (
    date DATE PRIMARY KEY,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    day_of_week VARCHAR(20) NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    season VARCHAR(30) NOT NULL,
    loaded_at TIMESTAMPTZ DEFAULT now()
);"""

CREATE_FACT_DAILY_WEATHER = f"""
CREATE TABLE IF NOT EXISTS {config.THIRD_TABLE} (
    city_id BIGINT NOT NULL REFERENCES {config.FIRST_TABLE}(city_id),
    weather_date DATE NOT NULL REFERENCES {config.SECOND_TABLE}(date),
    temp_max DOUBLE PRECISION NOT NULL,
    temp_min DOUBLE PRECISION NOT NULL,
    temp_avg DOUBLE PRECISION NOT NULL,
    precipitation DOUBLE PRECISION NOT NULL,
    rain DOUBLE PRECISION NOT NULL,
    wind_max DOUBLE PRECISION NOT NULL,
    is_rainy_day BOOLEAN NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (city_id, weather_date)
);"""

CREATE_REJECTED_RECORDS = f"""
CREATE TABLE IF NOT EXISTS {config.FOURTH_TABLE} (
    id SERIAL PRIMARY KEY,
    city_id BIGINT,
    town VARCHAR(100),
    weather_date DATE,
    temp_max DOUBLE PRECISION,
    temp_min DOUBLE PRECISION,
    precipitation DOUBLE PRECISION,
    rain DOUBLE PRECISION,
    wind_max DOUBLE PRECISION,
    rejection_reason TEXT NOT NULL,
    rejected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);"""

CREATE_ETL_WATERMARK = f"""
CREATE TABLE IF NOT EXISTS {config.FIFTH_TABLE} (
    city_id BIGINT PRIMARY KEY REFERENCES {config.FIRST_TABLE}(city_id),
    last_loaded_date DATE NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);"""

CREATE_ETL_RUN_LOG = f"""
CREATE TABLE IF NOT EXISTS {config.SIXTH_TABLE} (
    run_id SERIAL PRIMARY KEY,
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ,
    rows_extracted INTEGER DEFAULT 0,
    rows_loaded INTEGER DEFAULT 0,
    rows_rejected INTEGER DEFAULT 0,
    status VARCHAR(20) NOT NULL,
    error_message TEXT
);"""


def create_tables(engine):
    """Create all six tables in foreign-key dependency order."""
    statements = [
        CREATE_DIM_CITY,
        CREATE_DIM_DATE,
        CREATE_FACT_DAILY_WEATHER,
        CREATE_ETL_WATERMARK,
        CREATE_REJECTED_RECORDS,
        CREATE_ETL_RUN_LOG,
    ]
    with engine.begin() as conn:
        for stmt in statements:
            conn.execute(text(stmt))
    logger.info("All 6 tables are ready.")


# ---------------------------------------------------------------------------
# 3. Helpers
# ---------------------------------------------------------------------------
def _records(df: pd.DataFrame) -> list[dict]:
    """Convert DataFrame to list of dicts with NaN/NaT replaced by None."""
    if df is None or df.empty:
        return []
    return df.astype(object).where(df.notna(), None).to_dict("records")


def _safe_len(df) -> int:
    """len() that tolerates None."""
    return 0 if df is None else len(df)


# ---------------------------------------------------------------------------
# 4. Incremental Watermark Logic (Section 4.4)
# ---------------------------------------------------------------------------
def get_watermarks(engine) -> dict:
    """Return {city_id: last_loaded_date} for towns already loaded."""
    create_tables(engine)
    with engine.connect() as conn:
        rows = conn.execute(
            text(f"SELECT city_id, last_loaded_date FROM {config.FIFTH_TABLE}")
        ).fetchall()
    return {r[0]: r[1] for r in rows}


def get_date_window(watermark: date | None):
    """
    First run  : START_DATE -> today - DELAY_DAYS
    Later runs : watermark + 1 day -> today - DELAY_DAYS
    Returns None if nothing new needs to be fetched.
    """
    end = date.today() - timedelta(days=config.DELAY_DAYS)
    start = date.fromisoformat(config.START_DATE) if watermark is None else watermark + timedelta(days=1)
    return None if start > end else (start, end)


# ---------------------------------------------------------------------------
# 5. UPSERTs (Each runs inside caller's transaction)
# ---------------------------------------------------------------------------
def upsert_dim_city(conn, df: pd.DataFrame) -> int:
    recs = _records(df)
    if not recs:
        return 0
    conn.execute(text(f"""
        INSERT INTO {config.FIRST_TABLE}
            (city_id, name, county, latitude, longitude, elevation, population)
        VALUES
            (:city_id, :name, :county, :latitude, :longitude, :elevation, :population)
        ON CONFLICT (city_id) DO UPDATE SET
            name = EXCLUDED.name,
            county = EXCLUDED.county,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            elevation = EXCLUDED.elevation,
            population = EXCLUDED.population,
            loaded_at = now();
    """), recs)
    return len(recs)


def upsert_dim_date(conn, df: pd.DataFrame) -> int:
    """Returns rows attempted (existing dates are skipped by DO NOTHING)."""
    recs = _records(df)
    if not recs:
        return 0
    conn.execute(text(f"""
        INSERT INTO {config.SECOND_TABLE}
            (date, year, month, month_name, day_of_week, is_weekend, season)
        VALUES
            (:date, :year, :month, :month_name, :day_of_week, :is_weekend, :season)
        ON CONFLICT (date) DO NOTHING;
    """), recs)
    return len(recs)


def upsert_fact(conn, df: pd.DataFrame) -> int:
    recs = _records(df)
    if not recs:
        return 0
    now = datetime.now(timezone.utc)
    for r in recs:
        r["loaded_at"] = now
    conn.execute(text(f"""
        INSERT INTO {config.THIRD_TABLE}
            (city_id, weather_date, temp_max, temp_min, temp_avg,
             precipitation, rain, wind_max, is_rainy_day, loaded_at)
        VALUES
            (:city_id, :weather_date, :temp_max, :temp_min, :temp_avg,
             :precipitation, :rain, :wind_max, :is_rainy_day, :loaded_at)
        ON CONFLICT (city_id, weather_date) DO UPDATE SET
            temp_max = EXCLUDED.temp_max,
            temp_min = EXCLUDED.temp_min,
            temp_avg = EXCLUDED.temp_avg,
            precipitation = EXCLUDED.precipitation,
            rain = EXCLUDED.rain,
            wind_max = EXCLUDED.wind_max,
            is_rainy_day = EXCLUDED.is_rainy_day,
            loaded_at = EXCLUDED.loaded_at;
    """), recs)
    return len(recs)


def upsert_watermarks(conn, fact_df: pd.DataFrame) -> int:
    """Move each city's watermark forward to its max loaded weather_date."""
    if fact_df is None or fact_df.empty:
        return 0
    max_dates = fact_df.groupby("city_id")["weather_date"].max().to_dict()
    recs = [{"city_id": int(cid), "last_loaded_date": dt} for cid, dt in max_dates.items()]
    conn.execute(text(f"""
        INSERT INTO {config.FIFTH_TABLE} (city_id, last_loaded_date, updated_at)
        VALUES (:city_id, :last_loaded_date, now())
        ON CONFLICT (city_id) DO UPDATE SET
            last_loaded_date = GREATEST({config.FIFTH_TABLE}.last_loaded_date, EXCLUDED.last_loaded_date),
            updated_at = now();
    """), recs)
    return len(recs)


def insert_rejected(conn, df: pd.DataFrame) -> int:
    recs = _records(df)
    if not recs:
        return 0
    now = datetime.now(timezone.utc)
    for r in recs:
        r["rejected_at"] = now
    conn.execute(text(f"""
        INSERT INTO {config.FOURTH_TABLE}
            (city_id, town, weather_date, temp_max, temp_min,
             precipitation, rain, wind_max, rejection_reason, rejected_at)
        VALUES
            (:city_id, :town, :weather_date, :temp_max, :temp_min,
             :precipitation, :rain, :wind_max, :rejection_reason, :rejected_at);
    """), recs)
    return len(recs)


# ---------------------------------------------------------------------------
# 6. Atomic Pipeline Load + Run Log
# ---------------------------------------------------------------------------
def load_all(engine, dim_city, dim_date, fact_df, rejected_df) -> int:
    """Load all tables and advance watermarks in ONE atomic transaction."""
    create_tables(engine)
    start_time = datetime.now(timezone.utc)
    rows_extracted = _safe_len(fact_df) + _safe_len(rejected_df)

    try:
        with engine.begin() as conn:
            upsert_dim_city(conn, dim_city)
            upsert_dim_date(conn, dim_date)
            loaded = upsert_fact(conn, fact_df)
            upsert_watermarks(conn, fact_df)
            rejected = insert_rejected(conn, rejected_df)

        with engine.begin() as conn:
            conn.execute(text(f"""
                INSERT INTO {config.SIXTH_TABLE}
                    (start_time, end_time, rows_extracted, rows_loaded, rows_rejected, status)
                VALUES (:start, now(), :ext, :ld, :rej, 'SUCCESS');
            """), {"start": start_time, "ext": rows_extracted, "ld": loaded, "rej": rejected})

        logger.info("Load complete: %d loaded, %d rejected.", loaded, rejected)
        return loaded

    except SQLAlchemyError as e:
        logger.error("Load failed, transaction rolled back: %s", e)
        # The failed transaction is rolled back, so log the failure in a new one.
        try:
            with engine.begin() as conn:
                conn.execute(text(f"""
                    INSERT INTO {config.SIXTH_TABLE}
                        (start_time, end_time, rows_extracted, rows_loaded, rows_rejected, status, error_message)
                    VALUES (:start, now(), :ext, 0, 0, 'FAILED', :err);
                """), {"start": start_time, "ext": rows_extracted, "err": str(e)[:2000]})
        except SQLAlchemyError:
            logger.exception("Could not write FAILED row to run log.")
        raise


# ---------------------------------------------------------------------------
# 7. Pipeline entry point
# ---------------------------------------------------------------------------
JSON_PATH = "full_weather_data.json"


def main(fresh: bool = False) -> None:
    """Extract (if needed) -> transform -> load.

    Uses the saved JSON unless fresh=True or the file doesn't exist.
    Run `python load.py --fresh` to force a new extraction.
    """
    if fresh or not os.path.exists(JSON_PATH):
        logger.info("Running fresh extraction...")
        run_extraction()

    with open(JSON_PATH, "r", encoding="utf-8") as f:
        weather_results = json.load(f)

    dim_city, dim_date, fact_df, rejected_df = run_transformations(weather_results)

    if fact_df.empty and rejected_df.empty:
        logger.info("Nothing new to load.")
        return

    engine = get_engine()
    load_all(engine, dim_city, dim_date, fact_df, rejected_df)


if __name__ == "__main__":
    main(fresh="--fresh" in sys.argv)