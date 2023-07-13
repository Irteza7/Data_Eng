from datetime import datetime
import pandas as pd

def parse_data(data):
    # Convert epoch timestamps to datetime objects, and then to ISO 8601 strings
    timestamp_epoch = data.get('dt')
    if timestamp_epoch is not None:
        timestamp = datetime.utcfromtimestamp(timestamp_epoch).isoformat()
    else:
        timestamp = None

    sunrise_epoch = data['sys'].get('sunrise') if 'sys' in data else None
    if sunrise_epoch is not None:
        sunrise = datetime.utcfromtimestamp(sunrise_epoch).isoformat()
    else:
        sunrise = None

    sunset_epoch = data['sys'].get('sunset') if 'sys' in data else None
    if sunset_epoch is not None:
        sunset = datetime.utcfromtimestamp(sunset_epoch).isoformat()
    else:
        sunset = None

    # Parse the rest of the data as before
    parsed_data = {
        'timestamp': timestamp,
        'timezone': data.get('timezone'),
        'city_id': data.get('id'),
        'city_name': data.get('name'),
        'lon': data['coord'].get('lon') if 'coord' in data else None,
        'lat': data['coord'].get('lat') if 'coord' in data else None,
        'weather_id': data['weather'][0].get('id') if 'weather' in data and data['weather'] else None,
        'weather_main': data['weather'][0].get('main') if 'weather' in data and data['weather'] else None,
        'weather_description': data['weather'][0].get('description') if 'weather' in data and data['weather'] else None,
        'weather_icon': data['weather'][0].get('icon') if 'weather' in data and data['weather'] else None,
        'temp': data['main'].get('temp') if 'main' in data else None,
        'feels_like': data['main'].get('feels_like') if 'main' in data else None,
        'temp_min': data['main'].get('temp_min') if 'main' in data else None,
        'temp_max': data['main'].get('temp_max') if 'main' in data else None,
        'pressure': data['main'].get('pressure') if 'main' in data else None,
        'humidity': data['main'].get('humidity') if 'main' in data else None,
        'wind_speed': data['wind'].get('speed') if 'wind' in data else None,
        'wind_deg': data['wind'].get('deg') if 'wind' in data else None,
        'cloud_coverage': data['clouds'].get('all') if 'clouds' in data else None,
        'country': data['sys'].get('country') if 'sys' in data else None,
        'sunrise': sunrise,
        'sunset': sunset,
        'gust': data['wind'].get('gust') if 'wind' in data else None,
        'sea_level': data['main'].get('sea_level') if 'main' in data else None,
        'ground_level': data['main'].get('grnd_level') if 'main' in data else None
    }
    return parsed_data


def json_to_df(data):
    # Since some fields like 'weather', 'main', 'wind', 'clouds', 'sys' contain nested data, we need to flatten it
    # 'weather' is a list of dicts, but we'll only take the first element for simplicity
    flattened_data = data.copy()
    if 'coord' in data:
        flattened_data.update(flattened_data.pop('coord'))
    if 'weather' in data and len(data['weather']) > 0:
        weather_data = flattened_data.pop('weather')[0]
        flattened_data['weather_id'] = weather_data.get('id')
        flattened_data['weather_main'] = weather_data.get('main')
        flattened_data['weather_description'] = weather_data.get('description')
        flattened_data['weather_icon'] = weather_data.get('icon')
    if 'main' in data:
        flattened_data.update(flattened_data.pop('main'))
    if 'wind' in data:
        flattened_data.update(flattened_data.pop('wind'))
    if 'clouds' in data:
        flattened_data.update(flattened_data.pop('clouds'))
    if 'sys' in data:
        flattened_data.update(flattened_data.pop('sys'))
    # Convert the flattened data into a DataFrame
    df = pd.json_normalize(flattened_data)
    return df
