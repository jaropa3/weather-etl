import openmeteo_requests
import requests_cache
from retry_requests import retry


def fetch_weather_responses(locations, past_days=92, forecast_days=7):
	cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
	retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
	openmeteo = openmeteo_requests.Client(session=retry_session)

	url = "https://api.open-meteo.com/v1/forecast"
	params = {
		"latitude": [loc["latitude"] for loc in locations],
		"longitude": [loc["longitude"] for loc in locations],
		"hourly": ["temperature_2m", "rain", "snowfall", "cloud_cover", "visibility", "weather_code"],
		"past_days": past_days,
		"forecast_days": forecast_days,
	}
	return openmeteo.weather_api(url, params=params)
