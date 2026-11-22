import os
import json
import pandas as pd
from datetime import datetime, timezone
from extract import main as run_extraction


def transform_dim_city(weather_results: dict) -> pd.DataFrame:
    """Builds dim_city directly from weather_results[town]['geodata']."""
    city_rows = []

    for town in weather_results:
        location = weather_results[town]["geodata"]
        city_rows.append({
            "city_id": location["id"],
            "name": location["name"],
            "county": location["admin1"],
            "latitude": float(location["latitude"]),
            "longitude": float(location["longitude"]),
            "elevation": round(float(location["elevation"]), 1),
            "population": int(location["population"]),
        })

    return (
        pd.DataFrame(city_rows)
        .drop_duplicates(subset=["city_id"])
        .reset_index(drop=True)
    )


def reshape_and_validate_weather(weather_results: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Reshapes weather_results[town]['weather']['daily'] column lists into one dictionary
    per town per day, validates against Section 4.2 rules, derives fields, and rounds to 1 decimal.
    """
    valid_records = []
    rejected_records = []
    seen_keys = set()
    loaded_at = datetime.now(timezone.utc)

    for town in weather_results:
        location = weather_results[town]["geodata"]
        weather_data = weather_results[town]["weather"]

        city_id = location["id"]
        daily = weather_data["daily"]

        dates = daily["time"]
        temp_max_list = daily["temperature_2m_max"]
        temp_min_list = daily["temperature_2m_min"]
        precip_list = daily["precipitation_sum"]
        rain_list = daily["rain_sum"]
        wind_list = daily["wind_speed_10m_max"]

        # Reshape: turn column lists into one dictionary per town per day using positional index i
        for i in range(len(dates)):
            daily_record = {
                "city_id": city_id,
                "town": town,
                "weather_date": dates[i],
                "temp_max": temp_max_list[i],
                "temp_min": temp_min_list[i],
                "precipitation": precip_list[i],
                "rain": rain_list[i],
                "wind_max": wind_list[i],
            }

            reasons = []

            # Rule 1: A required value is missing
            required_fields = ["weather_date", "temp_max", "temp_min", "precipitation", "rain", "wind_max"]
            missing = [f for f in required_fields if daily_record[f] is None or pd.isna(daily_record[f])]
            if missing:
                reasons.append(f"Missing required value(s): {', '.join(missing)}")
            else:
                t_max = float(daily_record["temp_max"])
                t_min = float(daily_record["temp_min"])
                precip = float(daily_record["precipitation"])
                rain = float(daily_record["rain"])

                # Rule 2: Max temperature is below min
                if t_max < t_min:
                    reasons.append(f"Max temperature ({t_max}°C) is below min ({t_min}°C)")

                # Rule 3: Temperature outside -5 °C to 50 °C
                if not (-5.0 <= t_max <= 50.0) or not (-5.0 <= t_min <= 50.0):
                    reasons.append(f"Temperature outside -5°C to 50°C range (max={t_max}, min={t_min})")

                # Rule 4: Precipitation or rain is negative
                if precip < 0 or rain < 0:
                    reasons.append(f"Negative precipitation/rain (precipitation={precip} mm, rain={rain} mm)")

            # Rule 5: Same town and date appear twice in one batch
            batch_key = (city_id, daily_record["weather_date"])
            if batch_key in seen_keys:
                reasons.append(f"Duplicate record for {town} on {daily_record['weather_date']}")
            else:
                seen_keys.add(batch_key)

            if reasons:
                rejected_entry = daily_record.copy()
                rejected_entry["rejection_reason"] = "; ".join(reasons)
                rejected_entry["rejected_at"] = loaded_at
                rejected_records.append(rejected_entry)
                continue

            # Derive & round decimals consistently to 1 decimal place
            t_max_rounded = round(float(daily_record["temp_max"]), 1)
            t_min_rounded = round(float(daily_record["temp_min"]), 1)
            temp_avg = round((t_max_rounded + t_min_rounded) / 2, 1)
            precip_rounded = round(float(daily_record["precipitation"]), 1)
            rain_rounded = round(float(daily_record["rain"]), 1)
            wind_rounded = round(float(daily_record["wind_max"]), 1)

            valid_records.append({
                "city_id": city_id,
                "weather_date": daily_record["weather_date"],
                "temp_max": t_max_rounded,
                "temp_min": t_min_rounded,
                "temp_avg": temp_avg,
                "precipitation": precip_rounded,
                "rain": rain_rounded,
                "wind_max": wind_rounded,
                "is_rainy_day": precip_rounded >= 1.0,
                "loaded_at": loaded_at,
            })

    fact_cols = [
        "city_id", "weather_date", "temp_max", "temp_min", "temp_avg",
        "precipitation", "rain", "wind_max", "is_rainy_day", "loaded_at"
    ]
    rejected_cols = [
        "city_id", "town", "weather_date", "temp_max", "temp_min",
        "precipitation", "rain", "wind_max", "rejection_reason", "rejected_at"
    ]

    fact_daily_weather = pd.DataFrame(valid_records, columns=fact_cols)
    rejected_records_df = pd.DataFrame(rejected_records, columns=rejected_cols)
    return fact_daily_weather, rejected_records_df


def transform_dim_date(fact_daily_weather: pd.DataFrame) -> pd.DataFrame:
    """Builds dim_date with Kenyan seasons: Long rains (Mar-May), Short rains (Oct-Dec), Dry season (other)."""
    def get_kenyan_season(month: int) -> str:
        if month in (3, 4, 5):
            return "Long rains"
        if month in (10, 11, 12):
            return "Short rains"
        return "Dry season"

    unique_dates = pd.to_datetime(fact_daily_weather["weather_date"].unique())

    return (
        pd.DataFrame({
            "date": unique_dates,
            "year": unique_dates.year,
            "month": unique_dates.month,
            "month_name": unique_dates.month_name(),
            "day_of_week": unique_dates.day_name(),
            "is_weekend": unique_dates.dayofweek >= 5,
            "season": [get_kenyan_season(m) for m in unique_dates.month],
        })
        .sort_values("date")
        .reset_index(drop=True)
    )


def enforce_datatypes(
    dim_city: pd.DataFrame,
    dim_date: pd.DataFrame,
    fact_daily_weather: pd.DataFrame,
    rejected_records: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Explicitly casts every column across all four DataFrames to its target schema datatype."""

    # 1. dim_city
    dim_city = dim_city.astype({
        "city_id": "int64",
        "name": "string",
        "county": "string",
        "latitude": "float64",
        "longitude": "float64",
        "elevation": "float64",
        "population": "int64",
    })

    # 2. dim_date
    dim_date["date"] = pd.to_datetime(dim_date["date"]).dt.date
    dim_date = dim_date.astype({
        "year": "int32",
        "month": "int32",
        "month_name": "string",
        "day_of_week": "string",
        "is_weekend": "bool",
        "season": "string",
    })

    # 3. fact_daily_weather
    fact_daily_weather["weather_date"] = pd.to_datetime(fact_daily_weather["weather_date"]).dt.date
    fact_daily_weather["loaded_at"] = pd.to_datetime(fact_daily_weather["loaded_at"], utc=True)
    fact_daily_weather = fact_daily_weather.astype({
        "city_id": "int64",
        "temp_max": "float64",
        "temp_min": "float64",
        "temp_avg": "float64",
        "precipitation": "float64",
        "rain": "float64",
        "wind_max": "float64",
        "is_rainy_day": "bool",
    })

    # 4. rejected_records
    rejected_records["weather_date"] = pd.to_datetime(rejected_records["weather_date"], errors="coerce").dt.date
    rejected_records["rejected_at"] = pd.to_datetime(rejected_records["rejected_at"], utc=True)
    rejected_records = rejected_records.astype({
        "city_id": "Int64",
        "town": "string",
        "temp_max": "float64",
        "temp_min": "float64",
        "precipitation": "float64",
        "rain": "float64",
        "wind_max": "float64",
        "rejection_reason": "string",
    })

    return dim_city, dim_date, fact_daily_weather, rejected_records


def run_transformations(weather_results: dict) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Runs all transformations and returns (dim_city, dim_date, fact_daily_weather, rejected_records)."""
    dim_city = transform_dim_city(weather_results)
    fact_daily_weather, rejected_records = reshape_and_validate_weather(weather_results)
    dim_date = transform_dim_date(fact_daily_weather)

    return enforce_datatypes(dim_city, dim_date, fact_daily_weather, rejected_records)


if __name__ == "__main__":
    choice = input("1 = Load full_weather_data.json | 2 = Run fresh extraction: ").strip()

    if choice == "2" or not os.path.exists("full_weather_data.json"):
        run_extraction()

    with open("full_weather_data.json", "r", encoding="utf-8") as f:
        weather_results = json.load(f)

    dim_city, dim_date, fact_daily_weather, rejected_records = run_transformations(weather_results)

    pd.set_option("display.max_rows", None)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 1000)

    output_dir = "transformed"
    os.makedirs(output_dir, exist_ok=True)

    tables = {
        "dim_city": dim_city,
        "dim_date": dim_date,
        "fact_daily_weather": fact_daily_weather,
        "rejected_records": rejected_records
    }

    for name, df in tables.items():
        print(f"\n=== {name} ({len(df)} rows) ===")
        print(df.head(10).to_string(index=False))

        file_path = os.path.join(output_dir, f"{name}.csv")
        df.to_csv(file_path, index=False)
        print(f"Saved -> {file_path}")

    print(f"\nValidation summary: {len(rejected_records)} rejected record(s).")