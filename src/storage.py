from pathlib import Path

import pandas as pd


def _is_gcs(path: str) -> bool:
    return path.startswith("gs://")


def _join(base: str, *parts: str) -> str:
    if _is_gcs(base):
        return base.rstrip("/") + "/" + "/".join(parts)
    return str(Path(base, *parts))


def _ensure_dir(path: str) -> None:
    if not _is_gcs(path):
        Path(path).mkdir(parents=True, exist_ok=True)


def write_raw_partition(df: pd.DataFrame, base_dir: str, location_name: str, run_date: str) -> str:
    """Write df to <base_dir>/dt=<run_date>/weather_<name>.parquet (overwrites)."""
    partition_dir = _join(base_dir, f"dt={run_date}")
    _ensure_dir(partition_dir)
    path = _join(partition_dir, f"weather_{location_name}.parquet")
    df.to_parquet(path, engine="pyarrow", index=False)
    return path


def read_raw_history(base_dir: str, location_name: str) -> pd.DataFrame:
    """Concat every raw partition for the given location, oldest first."""
    pattern = f"dt=*/weather_{location_name}.parquet"

    if _is_gcs(base_dir):
        import gcsfs
        fs = gcsfs.GCSFileSystem()
        files = sorted(fs.glob(f"{base_dir.rstrip('/')}/{pattern}"))
        if not files:
            return pd.DataFrame()
        return pd.concat([pd.read_parquet(f"gs://{f}") for f in files], ignore_index=True)

    files = sorted(Path(base_dir).glob(pattern))
    if not files:
        return pd.DataFrame()
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


def write_staging(df: pd.DataFrame, staging_dir: str, location_name: str) -> str:
    """Overwrite staging file for the given location."""
    _ensure_dir(staging_dir)
    path = _join(staging_dir, f"weather_{location_name}.parquet")
    df.to_parquet(path, engine="pyarrow", index=False)
    return path
