from datetime import date
from pathlib import Path

import yaml

from client import fetch_weather_responses
from storage import read_raw_history, write_raw_partition, write_staging
from transform import parse_responses, validate_weather

SRC_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SRC_DIR.parent
DEFAULT_CONFIG_PATH = SRC_DIR / "config.yaml"


def load_config(config_path: Path = DEFAULT_CONFIG_PATH) -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def run() -> None:
    config = load_config()
    raw_dir = PROJECT_ROOT / config["paths"]["raw_data"]
    staging_dir = PROJECT_ROOT / config["paths"]["staging_data"]
    run_date = date.today().isoformat()

    responses = fetch_weather_responses(config["locations"])
    items = parse_responses(config["locations"], responses)

    # 1. Raw — append-only daily partition, untouched API output.
    for item in items:
        path = write_raw_partition(item["df"], raw_dir, item["name"], run_date)
        print(f"[{item['name']}] raw  -> {path} ({len(item['df'])} rows)")

    # 2. Staging — read full raw history, validate, dedup by date, overwrite.
    for item in items:
        history = read_raw_history(raw_dir, item["name"])
        if history.empty:
            print(f"[{item['name']}] skipping staging: no raw history")
            continue

        df = validate_weather(history, name=item["name"])
        df = (
            df.drop_duplicates(subset=["date"], keep="last")
            .sort_values("date")
            .reset_index(drop=True)
        )
        if df.empty:
            print(f"[{item['name']}] skipping staging: empty after validation")
            continue

        path = write_staging(df, staging_dir, item["name"])
        print(f"[{item['name']}] stage -> {path} ({len(df)} rows)")
