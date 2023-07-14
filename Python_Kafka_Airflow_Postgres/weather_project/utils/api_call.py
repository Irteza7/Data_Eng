import requests
from utils.settings import get_settings

# Get the settings
settings = get_settings()

def fetch_weather_data(city):
    # `api_key` is now accessed from `settings`
    params = {'appid': settings["api_key"], 'q': city, 'units': 'metric'}
    response = requests.get('http://api.openweathermap.org/data/2.5/weather', params=params)

    if response.status_code != 200:
        raise Exception(f"Failed to fetch data from API for city {city}: {response.content}")

    return response.json()
