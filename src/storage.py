from pathlib import Path

import pandas as pd


def write_raw_partition(df: pd.DataFrame, base_dir: Path, location_name: str, run_date: str) -> Path:
    """Write df to <base_dir>/dt=<run_date>/weather_<name>.parquet (overwrites)."""
    partition_dir = base_dir / f"dt={run_date}"
    partition_dir.mkdir(parents=True, exist_ok=True)
    path = partition_dir / f"weather_{location_name}.parquet"
    df.to_parquet(path, engine="pyarrow", index=False)
    return path


def read_raw_history(base_dir: Path, location_name: str) -> pd.DataFrame:
    """Concat every raw partition for the given location, oldest first."""
    files = sorted(base_dir.glob(f"dt=*/weather_{location_name}.parquet"))
    if not files:
        return pd.DataFrame()
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


def write_staging(df: pd.DataFrame, staging_dir: Path, location_name: str) -> Path:
    """Overwrite staging file for the given location."""
    staging_dir.mkdir(parents=True, exist_ok=True)
    path = staging_dir / f"weather_{location_name}.parquet"
    df.to_parquet(path, engine="pyarrow", index=False)
    return path
