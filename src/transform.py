import pandas as pd

HOURLY_VARS = [
    "temperature_2m",
    "rain",
    "snowfall",
    "cloud_cover",
    "visibility",
    "weather_code",
]

WEATHER_DTYPES = {
    "date": "datetime64[us, UTC]",
    "temperature_2m": "float32",
    "rain": "float32",
    "snowfall": "float32",
    "cloud_cover": "float32",
    "visibility": "float32",
    "weather_code": "int16",
}

ROUND_DECIMALS = {
    "temperature_2m": 2,
    "rain": 2,
}


def response_to_dataframe(response) -> pd.DataFrame:
    hourly = response.Hourly()
    data = {"date": pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left",
    )}
    for idx, name in enumerate(HOURLY_VARS):
        data[name] = hourly.Variables(idx).ValuesAsNumpy()
    return pd.DataFrame(data)


def parse_responses(locations, responses):
    items = []
    for loc, response in zip(locations, responses):
        items.append({
            "name": loc["name"],
            "latitude": response.Latitude(),
            "longitude": response.Longitude(),
            "elevation": response.Elevation(),
            "df": response_to_dataframe(response),
        })
    return items


def data_cleaning(df):
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )
    return df


def validate_weather(df: pd.DataFrame, name: str = "") -> pd.DataFrame:
    expected = set(WEATHER_DTYPES.keys())
    missing = expected - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns: {sorted(missing)}")

    before = len(df)
    df = df.dropna().copy()
    dropped = before - len(df)
    if dropped:
        prefix = f"[{name}] " if name else ""
        print(f"{prefix}dropped {dropped} NaN rows ({before} -> {len(df)})")

    df["date"] = pd.to_datetime(df["date"], utc=True)
    for col, dtype in WEATHER_DTYPES.items():
        df[col] = df[col].astype(dtype)
    for col, decimals in ROUND_DECIMALS.items():
        df[col] = df[col].round(decimals)

    return df
