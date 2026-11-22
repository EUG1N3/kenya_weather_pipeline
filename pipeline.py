"""Kenya weather ETL orchestrator: extract -> transform -> load.

Usage:
    python pipeline.py                  # fresh extraction, then transform + load
    python pipeline.py --skip-extract   # reuse the existing full_weather_data.json
"""
import os
import sys
import json
import time
import logging
import argparse
from pathlib import Path

# extract.py writes full_weather_data.json to a relative path, so always run
# from this folder (matters when started by Task Scheduler or from elsewhere).
os.chdir(Path(__file__).resolve().parent)

import extract
import transform
import load

logger = logging.getLogger("pipeline")
JSON_PATH = "full_weather_data.json"


def step_extract(skip: bool) -> dict:
    """Step 1: pull geodata + weather from Open-Meteo and read the saved JSON."""
    if skip and os.path.exists(JSON_PATH):
        logger.info("[1/3] Extract skipped, using existing %s", JSON_PATH)
    else:
        logger.info("[1/3] Extracting from Open-Meteo...")
        try:
            extract.main()
        except SystemExit as e:
            # extract.py calls sys.exit(1) on network/HTTP errors
            raise RuntimeError("Extraction failed (see the message printed above).") from e

    with open(JSON_PATH, "r", encoding="utf-8") as f:
        weather_results = json.load(f)

    if not weather_results:
        raise RuntimeError("Extraction returned no towns.")

    logger.info("[1/3] Extract done: %d town(s)", len(weather_results))
    return weather_results


def step_transform(weather_results: dict):
    """Step 2: validate, reshape and build the dimension/fact tables."""
    logger.info("[2/3] Transforming...")
    dim_city, dim_date, fact_df, rejected_df = transform.run_transformations(weather_results)
    logger.info(
        "[2/3] Transform done: %d cities, %d dates, %d valid rows, %d rejected rows",
        len(dim_city), len(dim_date), len(fact_df), len(rejected_df),
    )
    return dim_city, dim_date, fact_df, rejected_df


def step_load(dim_city, dim_date, fact_df, rejected_df) -> int:
    """Step 3: load everything into PostgreSQL in one atomic transaction."""
    if fact_df.empty and rejected_df.empty:
        logger.info("[3/3] Nothing to load.")
        return 0

    logger.info("[3/3] Loading into PostgreSQL...")
    engine = load.get_engine()
    loaded = load.load_all(engine, dim_city, dim_date, fact_df, rejected_df)
    logger.info("[3/3] Load done: %d fact rows upserted", loaded)
    return loaded


def run_pipeline(skip_extract: bool = False) -> None:
    start = time.time()
    logger.info("Pipeline started")

    weather_results = step_extract(skip_extract)
    dim_city, dim_date, fact_df, rejected_df = step_transform(weather_results)
    step_load(dim_city, dim_date, fact_df, rejected_df)

    logger.info("Pipeline finished in %.1fs", time.time() - start)


def main() -> None:
    parser = argparse.ArgumentParser(description="Kenya weather ETL pipeline")
    parser.add_argument(
        "--skip-extract",
        action="store_true",
        help="reuse the existing full_weather_data.json instead of calling the API",
    )
    args = parser.parse_args()

    try:
        run_pipeline(skip_extract=args.skip_extract)
    except Exception:
        logger.exception("Pipeline failed")
        sys.exit(1)  # non-zero exit code so schedulers can detect the failure


if __name__ == "__main__":
    main()
