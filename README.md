# Kenya City Weather Pipeline

An ETL pipeline that pulls daily weather history for five Kenyan towns (Nairobi, Mombasa, Kisumu, Nakuru, Eldoret), cleans and validates it, and loads it into a **star schema in PostgreSQL (Aiven)**.

Built for the LuxDevHQ Data Engineering programme. The business goal is to let an agricultural advisory company query weather insights for farmers and county officers instead of looking up websites by hand.

**Questions the database answers**

1. Which town had the most rainfall each month?
2. How many rainy days did each town have in the long rains (Mar-May) and short rains (Oct-Dec)?
3. What were the hottest and coldest days in each town?

---

## Data source

Both sources come from [Open-Meteo](https://open-meteo.com/) (free for non-commercial use, no API key).

| Source | Used for |
|---|---|
| Geocoding API | Town id, latitude, longitude, elevation, county (`admin1`), population |
| Historical Weather API | Daily max/min temperature, precipitation, rain, max wind speed |

> Weather data by [Open-Meteo.com](https://open-meteo.com/). Historical data is published with a delay of about 5 days, so the pipeline never requests anything later than **today minus 5 days**.

---

## Architecture

```
Geocoding API ──┐
                ├─► EXTRACT ─► TRANSFORM ─► LOAD (one transaction) ─► PostgreSQL
Weather API ────┘   extract.py  transform.py   load.py                 dim_city
                                                                       dim_date
                    full_weather_data.json                             fact_daily_weather
                    (raw snapshot)                                     etl_watermark
                                                                       rejected_records
Scheduler (cron / Task Scheduler) ─► pipeline.py (daily 06:00)         etl_run_log
Logging ─► logs/pipeline.log
```

### Star schema

```
dim_city (city_id PK)          dim_date (date PK)
      ▲                              ▲
      └──── fact_daily_weather ──────┘
            PK (city_id, weather_date)
```

| Table | Type | One row per |
|---|---|---|
| `dim_city` | Dimension | Town |
| `dim_date` | Dimension | Calendar day (built in Python, includes Kenyan season) |
| `fact_daily_weather` | Fact | Town per day |
| `etl_watermark` | Control | Town (last date loaded) |
| `rejected_records` | Quality | Bad record, with the reason |
| `etl_run_log` | Monitoring | Pipeline run |

The fact table uses a **composite primary key** `(city_id, weather_date)` because one row is unique by town and date together.

---

## What each step does

### Extract (`extract.py`)
- Geocodes each town once, filtered to Kenya (`countryCode=KE`).
- Requests daily weather from `START_DATE` (1 Jan 2025) to today minus 5 days.
- Saves the raw response for all towns to `full_weather_data.json`.

### Transform (`transform.py`)
- **Reshape:** the weather API returns one list per column (dates, max temps, rain...). These lists are zipped into one record per town per day.
- **Derive:** `temp_avg = (max + min) / 2`, `is_rainy_day = precipitation >= 1 mm`. All decimals rounded to 1 place.
- **Build `dim_date`:** year, month, month name, day of week, weekend flag, and season (Long rains Mar-May, Short rains Oct-Dec, Dry season otherwise).
- **Validate.** A record goes to `rejected_records` with a reason if:

| Rule | Example |
|---|---|
| A required value is missing | `temperature_2m_max` is null |
| Max temperature is below min | max 18.0, min 21.5 |
| A temperature is outside -5 to 50 °C | max 63.0 |
| Precipitation or rain is negative | -2.0 mm |
| Same town and date twice in one batch | two Nairobi rows for the same day |

### Load (`load.py`)
- Creates all six tables from Python, with foreign keys from the fact table to both dimensions.
- Loads in order: `dim_city` -> `dim_date` -> `fact_daily_weather`, then rejected rows and watermarks.
- Everything uses **UPSERT** (`ON CONFLICT`), so reruns never create duplicates.
- All tables and the watermark update run in **one transaction** (`engine.begin()`), so a failure rolls everything back.
- Writes one row to `etl_run_log` per load, `SUCCESS` or `FAILED` (the failure row is written in a separate transaction so the rollback does not erase it).

### Orchestrator (`pipeline.py`)
Runs Extract -> Transform -> Load in order, logs to `logs/pipeline.log` and the console, and exits with a non-zero code on failure so a scheduler can detect it.

---

## Project structure

```
kenya_weather_pipeline/
├── .env                  # secrets (never committed)
├── .gitignore
├── requirements.txt
├── config.py             # towns, API URLs, start date, delay days, table names
├── extract.py            # geocoding + weather requests
├── transform.py          # reshape, derive, validate, build dim_date
├── load.py               # DDL, UPSERTs, watermark, run log, one transaction
├── pipeline.py           # orchestrator
├── run_pipeline.sh       # launcher for cron (Linux / macOS / WSL)
├── run_pipeline.bat      # launcher for Task Scheduler (Windows)
├── full_weather_data.json
├── transformed/          # CSV output when running transform.py directly
├── logs/                 # pipeline.log and scheduler.log (not committed)
└── README.md
```

---

## Setup

**1. Install dependencies** (Python 3.10+)

```bash
python -m venv venv
source venv/bin/activate        # Windows PowerShell: venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

**2. Create a `.env` file** in the project root. Never commit it.

```env
GEO_URL=https://geocoding-api.open-meteo.com/v1/search
HISTORY_URL=https://archive-api.open-meteo.com/v1/archive

DB_NAME=your_database
DB_HOST=your-service.aivencloud.com
DB_PORT=your_port
DB_USER=your_user
DB_PASSWORD=your_password
SSL_MODE=require
```

**3. Check `.gitignore`** contains at least:

```
.env
venv/
__pycache__/
logs/
```

---

## Running the pipeline

```bash
python pipeline.py                  # fresh extraction, then transform and load
python pipeline.py --skip-extract   # reuse the saved full_weather_data.json
python transform.py                 # transform only, writes CSVs to transformed/
python load.py --fresh              # alternative entry point: load with fresh extraction
```

Running the pipeline twice in a row leaves the row counts unchanged because every write is an UPSERT.

---

## Scheduling a daily run

The pipeline is scheduled to run every day at **06:00**. Pick the section that matches your system.

> Do **not** schedule it with `--skip-extract`. That flag reuses the old JSON file, so no new days would ever arrive.

Two log files are written:

| File | Contains |
|---|---|
| `logs/pipeline.log` | Normal application logs (start, each step, finish, errors) |
| `logs/scheduler.log` | System-level errors that happen before Python logging starts (missing module, wrong path) |

### Option A: Linux, macOS or WSL (cron)

Cron runs in a minimal shell that does not activate virtual environments or set the working directory, so it calls a small launcher script.

**1. Prerequisites**

```bash
cd /path/to/kenya_weather_pipeline
sudo apt update && sudo apt install -y python3-venv python3-pip cron   # Debian / Ubuntu / WSL
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

If you copied the project from Windows, delete the old Windows virtual environment folder first (for example `rm -rf python-weather`), because Windows `.exe` binaries cannot run on Linux.

**2. Create the launcher `run_pipeline.sh`** (replace the path with your real project directory)

```bash
cat << 'EOF' > run_pipeline.sh
#!/bin/bash
PROJECT_DIR="/home/username/kenya_weather_pipeline"
cd "$PROJECT_DIR" || exit 1
mkdir -p logs
"$PROJECT_DIR/venv/bin/python" pipeline.py >> "$PROJECT_DIR/logs/scheduler.log" 2>&1
EOF
chmod +x run_pipeline.sh
```

**3. Test it manually before scheduling**

```bash
./run_pipeline.sh
cat logs/scheduler.log
```

Check that `logs/pipeline.log` has a clean start-to-finish block, then confirm a new row was added in the Aiven console:

```sql
SELECT * FROM etl_run_log ORDER BY run_id DESC LIMIT 5;
```

**4. Schedule it**

```bash
sudo service cron start      # start the cron service (WSL / some Linux setups)
crontab -e                   # choose nano if asked
```

Add this line at the bottom (use your real path), then save with `Ctrl+O`, `Enter`, `Ctrl+X`:

```
0 6 * * * /home/username/kenya_weather_pipeline/run_pipeline.sh
```

Confirm it is registered:

```bash
crontab -l
```

> **WSL note:** WSL suspends background processes shortly after all Linux terminal windows are closed. Keep an Ubuntu terminal open during scheduled runs, or use Task Scheduler (Option B) if you run the project on Windows.

### Option B: Windows (Task Scheduler)

`crontab` does not exist on Windows (it fails with `crontab: command not found` in Git Bash). Use Task Scheduler instead.

**1. Find your Python path** (with your virtual environment active)

```bash
python -c "import sys; print(sys.executable)"
```

**2. Create `run_pipeline.bat`** in the project folder, using your own paths

```bat
@echo off
cd /d C:\Users\YOU\Desktop\kenya_weather_pipeline
if not exist logs mkdir logs
"C:\PATH\TO\YOUR\python.exe" pipeline.py >> logs\scheduler.log 2>&1
```

**3. Create the daily task.** Run this in **PowerShell or Command Prompt, not Git Bash** (Git Bash rewrites `/Create`-style options and breaks `schtasks`):

```powershell
schtasks /Create /SC DAILY /ST 06:00 /TN "KenyaWeatherPipeline" /TR "C:\Users\YOU\Desktop\kenya_weather_pipeline\run_pipeline.bat"
```

**4. Test it now**

```powershell
schtasks /Run /TN "KenyaWeatherPipeline"
schtasks /Query /TN "KenyaWeatherPipeline" /V /FO LIST
```

Then check `logs\pipeline.log` for a fresh run and run the `etl_run_log` query above to see the new row.

**5. Handle missed runs.** In Task Scheduler open the task, go to **Properties > Settings**, and tick **Run task as soon as possible after a scheduled start is missed**, so a run still happens if the computer was off at 06:00.

### Confirming the schedule works

After a scheduled time has passed, all three should be true:

1. `logs/pipeline.log` has a new "Pipeline started ... finished" block.
2. `etl_run_log` has a new row.
3. The `fact_daily_weather` row count has not changed, apart from any genuinely new days.

---

## Verifying the load

Expected first run: **5 towns x every day from 2025-01-01 to today minus 5 days**.

```sql
-- days, first and last date per town
SELECT c.name, COUNT(*) AS days,
       MIN(f.weather_date) AS first_day, MAX(f.weather_date) AS last_day
FROM fact_daily_weather f
JOIN dim_city c ON c.city_id = f.city_id
GROUP BY c.name
ORDER BY c.name;

-- rejected records and run history
SELECT * FROM rejected_records ORDER BY rejected_at DESC LIMIT 20;
SELECT * FROM etl_run_log ORDER BY run_id DESC;
```

---

## Business queries

**1. Wettest town each month**

```sql
WITH monthly AS (
    SELECT d.year, d.month, d.month_name, c.name AS town,
           ROUND(SUM(f.precipitation)::numeric, 1) AS total_rain_mm,
           RANK() OVER (PARTITION BY d.year, d.month
                        ORDER BY SUM(f.precipitation) DESC) AS rnk
    FROM fact_daily_weather f
    JOIN dim_city c ON c.city_id = f.city_id
    JOIN dim_date d ON d.date = f.weather_date
    GROUP BY d.year, d.month, d.month_name, c.name
)
SELECT year, month_name, town, total_rain_mm
FROM monthly
WHERE rnk = 1
ORDER BY year, month;
```

**2. Rainy days per town per rainy season**

```sql
SELECT c.name AS town, d.year, d.season,
       COUNT(*) FILTER (WHERE f.is_rainy_day) AS rainy_days
FROM fact_daily_weather f
JOIN dim_city c ON c.city_id = f.city_id
JOIN dim_date d ON d.date = f.weather_date
WHERE d.season IN ('Long rains', 'Short rains')
GROUP BY c.name, d.year, d.season
ORDER BY c.name, d.year, d.season;
```

**3. Hottest and coldest day per town**

```sql
WITH ranked AS (
    SELECT c.name, f.weather_date, f.temp_max, f.temp_min,
           ROW_NUMBER() OVER (PARTITION BY f.city_id ORDER BY f.temp_max DESC) AS hot_rn,
           ROW_NUMBER() OVER (PARTITION BY f.city_id ORDER BY f.temp_min ASC)  AS cold_rn
    FROM fact_daily_weather f
    JOIN dim_city c ON c.city_id = f.city_id
)
SELECT name AS town, 'Hottest' AS day_type, weather_date, temp_max AS temperature_c
FROM ranked WHERE hot_rn = 1
UNION ALL
SELECT name, 'Coldest', weather_date, temp_min
FROM ranked WHERE cold_rn = 1
ORDER BY town, day_type;
```

---

## Design decisions (what to be able to explain)

1. **Why reshape columns to rows?** The API returns one list per variable. A database row needs one town, one date and all its measurements together, so the lists are zipped by position.
2. **Why a composite primary key?** A single date or town is not unique. Only the pair `(city_id, weather_date)` identifies one fact row, and it is also the conflict target for the UPSERT.
3. **Why load dimensions before the fact table, in one transaction?** The fact table has foreign keys to both dimensions, so the referenced rows must exist first. One transaction means either everything is loaded (including the watermark) or nothing is.
4. **Why the 5-day delay?** Open-Meteo has not published the most recent days yet, so requests stop at today minus 5.

---

## Current status and known limitations

Done:
- [x] Extract, transform and validation rules
- [x] Star schema created from Python with foreign keys
- [x] Composite-key UPSERTs in one transaction
- [x] Rejected records and run log tables
- [x] Secrets in `.env`, not in code
- [x] File logging to `logs/pipeline.log`
- [x] Daily scheduling (cron on Linux/macOS/WSL, Task Scheduler on Windows)

Still to do (from the project brief):
- [ ] **Retries with exponential backoff** (1s, 2s, 4s) in a reusable `get_json()`. Currently `extract.py` exits on the first network error.
- [ ] **Incremental extraction.** The watermark is written but not yet used to limit the request, so each run re-requests the full history (safe because of the UPSERT, but not efficient). `get_date_window()` in `load.py` is not wired in yet and expects `START_DATE` as a string while `config.py` stores a `date`.
- [ ] **Continue when one town fails** instead of stopping the whole run.
- [ ] **Log extract failures** to `etl_run_log` (today only the load step writes to it).
- [ ] **Per-town reconciliation:** extracted = loaded + rejected.
- [ ] Mombasa delete-and-reload test (delete the last 7 days, roll the watermark back, rerun).
- [ ] Stretch goals: `--add-city`, `--full-refresh`, monthly summary table, late-arriving data reload.

---

## Credits

Weather data provided by [Open-Meteo.com](https://open-meteo.com/) under their free non-commercial terms.