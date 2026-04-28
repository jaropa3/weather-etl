# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

- Run the pipeline: `python src/main.py` (from project root). `main.py` is a thin entry point — all work is in `src/pipeline.py:run()`.
- Install deps: `pip install -r requirements.txt` into `.venv/`. `pytest` is listed but no first-party tests exist yet.
- Output: `data/raw/dt=<run_date>/weather_<name>.parquet` (Hive-partitioned by ingest date) and `data/staging/weather_<name>.parquet` (validated + deduped, derivable from raw). Both directories are created on demand and gitignored.

## Architecture

ETL that pulls hourly weather from Open-Meteo and writes per-location Parquet files. Three concerns are explicitly separated; pipeline is the only module that knows about all of them:

**Transport** — [src/client.py](src/client.py). `fetch_weather_responses(locations, past_days=92, forecast_days=7)` is HTTP-only: wraps `openmeteo_requests.Client` with `requests_cache` (1h) + `retry_requests`, batches all locations into one multi-coordinate call (Open-Meteo preserves input order), returns the raw response objects. No DataFrames here.

**Transformation** — [src/transform.py](src/transform.py). Two stages:
- `parse_responses(locations, responses)` — pairs each Open-Meteo response with its location dict and builds the DataFrame via `response_to_dataframe()`. Iterates `HOURLY_VARS` (derived from `WEATHER_DTYPES`) so adding a field in client requires only updating `WEATHER_DTYPES`.
- `validate_weather(df, name)` — schema check, `dropna()` with logging, explicit dtype cast (`float32` for metrics, `int16` for `weather_code`, `datetime64[us, UTC]` for `date` — written as `timestamp[us, UTC]` in parquet). Then `ROUND_DECIMALS` rounds selected columns (`temperature_2m`, `rain` → 2 decimals). Any new field added to the API call must also land in `WEATHER_DTYPES` or validation will reject it; round configuration goes in `ROUND_DECIMALS`.

**I/O** — [src/storage.py](src/storage.py). Two-tier layout (medallion: raw / staging):
- `write_raw_partition(df, base, name, run_date)` → `data/raw/dt=<YYYY-MM-DD>/weather_<name>.parquet`. Hive-partitioned by ingest date, overwrites within the same day, append-only across days. No validation here — raw is the audit trail of "what the API returned".
- `read_raw_history(base, name)` — globs `dt=*/weather_<name>.parquet` and concats, oldest first.
- `write_staging(df, dir, name)` → `data/staging/weather_<name>.parquet`. Overwrites; staging is fully derivable from raw and can be regenerated at any time.

**Orchestration** — [src/pipeline.py](src/pipeline.py). `run()` is two passes: (1) write today's raw partition for every location (untouched); (2) per location read full raw history → `validate_weather` → `drop_duplicates(["date"], keep="last")` → sort → `write_staging`. `keep="last"` lets newer runs overwrite forecast hours once they become observations. Holds `load_config()` and path resolution; no business logic.

**Entry point** — [src/main.py](src/main.py). One import, one call. Keep CLI flags here, not in `pipeline.py`.

**Logging helper** — [src/logger_app.py](src/logger_app.py). `setup_logger()` writes to `logs/etl_YYYYMMDD.log`. **Not yet wired into the pipeline** — current code uses `print()`. If adding logging, route through this helper.

## Config

- [src/config.yaml](src/config.yaml) is the **real config** (`locations`, `paths.raw_data`, plus unused Supabase fields). Loaded via `yaml.safe_load` in `pipeline.load_config()`.
- [config/config.yaml](config/config.yaml) is **misnamed** — despite the `.yaml` extension it contains Python (`PATH_FOLDER = ...`). Do not `yaml.safe_load` it. It appears to be leftover from an earlier iteration and is not imported anywhere after the recent refactor.
- `src/config.yaml` currently holds a plaintext Supabase password. Treat as a known tech-debt item; don't add more secrets there.

## Unrelated artifacts

- [sql/schema.sql](sql/schema.sql) defines a retail schema (`dim_item`, `dim_store_name`, `final_merge`) that has no connection to the weather pipeline. It's from a different project phase — do not assume changes to the weather code need to touch it.

## Out-of-scope items discussed but not yet implemented

Keep these in mind when proposing changes so you don't accidentally re-litigate decisions already deferred:

- Explicit `pyarrow.Table` schema at write time (currently relies on `df.to_parquet(engine="pyarrow")` inferring from the validated dtypes).
- Multi-level Hive partitioning beyond `dt=` — e.g., `location=<name>/dt=<date>/` — would let DuckDB partition-prune by location too.
- `urllib3.util.Retry` + `HTTPAdapter` for status-aware retry and explicit connect/read timeouts (current `retry_requests` setup handles neither).
- Cursor-based pagination over date windows for backfills > 92 days.
