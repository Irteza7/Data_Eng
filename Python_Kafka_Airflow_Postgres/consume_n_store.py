from kafka import KafkaConsumer
from datetime import datetime
import json
import pyspark
import pandas as pd
import psycopg2
import configparser
import os

def parse_data(data):
    from datetime import datetime

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


current_directory = os.getcwd()
file_name = "Python_Kafka_Airflow_Postgres/config.ini"
file_path = os.path.join(current_directory, file_name)
config = configparser.ConfigParser()
config.read(file_path)

user_name = config.get('Postgres', 'POSTGRES_USER')
user_pass = config.get('Postgres','POSTGRES_PASSWORD')
database_name = "weather"

# Parameters for connecting to the PostgreSQL server
params = {
    "database": database_name,
    "host": "127.0.0.1",
    "port": "5432",
    "user": user_name,
    "password": user_pass
}


conn = psycopg2.connect(**params)
cursor = conn.cursor()

# Create table if not exists
cursor.execute("""
CREATE TABLE IF NOT EXISTS weather_data (
    timestamp TIMESTAMP,  -- corresponds to 'dt'
    timezone INT,  -- corresponds to 'timezone'
    city_id INT,  -- corresponds to 'id'
    city_name VARCHAR(50),  -- corresponds to 'name'
    lon FLOAT,  -- corresponds to 'lon'
    lat FLOAT,  -- corresponds to 'lat'
    weather_id INT,  -- corresponds to 'weather_id'
    weather_main VARCHAR(50),  -- corresponds to 'weather_main'
    weather_description VARCHAR(100),  -- corresponds to 'weather_description'
    weather_icon VARCHAR(50),  -- corresponds to 'weather_icon'
    temp FLOAT,  -- corresponds to 'temp'
    feels_like FLOAT,  -- corresponds to 'feels_like'
    temp_min FLOAT,  -- corresponds to 'temp_min'
    temp_max FLOAT,  -- corresponds to 'temp_max'
    pressure INT,  -- corresponds to 'pressure'
    humidity INT,  -- corresponds to 'humidity'
    wind_speed FLOAT,  -- corresponds to 'speed'
    wind_deg INT,  -- corresponds to 'deg'
    cloud_coverage INT,  -- corresponds to 'all'
    country VARCHAR(50),  -- corresponds to 'country'
    sunrise TIMESTAMP,  -- corresponds to 'sunrise'
    sunset TIMESTAMP,  -- corresponds to 'sunset'
    gust FLOAT,  -- corresponds to 'gust'
    sea_level FLOAT,  -- corresponds to 'sea_level'
    ground_level FLOAT  -- corresponds to 'grnd_level'
)
""")
conn.commit()

consumer = KafkaConsumer('weather_topic',
                         bootstrap_servers='localhost:9092',
                         auto_offset_reset='earliest',
                         group_id='weather_group')

# df = pd.DataFrame()  # Initialize an empty DataFrame

# try:
#     while True:
#         poll_result = consumer.poll(timeout_ms=5000)  # poll for messages, waiting up to 5 seconds
#         if not poll_result:  # if the poll result is empty, break the loop
#             break
#         for tp, messages in poll_result.items():
#             for message in messages:
#                 data = json.loads(message.value.decode('utf-8'))
#                 message_df = json_to_df(data)
#                 df = pd.concat([df, message_df])
#                 print(f"Consumed message. DataFrame now has {len(df)} row(s).")
# except Exception as e:
#     print(f"Error while consuming: {e}")
# # df.columns


try:
    while True:
        poll_result = consumer.poll(timeout_ms=10000)  # poll for messages, waiting up to 10 seconds
        if not poll_result:  # if the poll result is empty, break the loop
            break
        for tp, messages in poll_result.items():
            for message in messages:
                data = json.loads(message.value.decode('utf-8'))
                parsed_data = parse_data(data)
                # Insert the data into PostgreSQL
                cursor.execute("""
                INSERT INTO weather_data (timestamp, timezone, city_id, city_name, lon, lat, weather_id, weather_main, weather_description, weather_icon, temp, feels_like, temp_min, temp_max, pressure, humidity, wind_speed, wind_deg, cloud_coverage, country, sunrise, sunset, gust, sea_level, ground_level) 
                VALUES (%(timestamp)s, %(timezone)s, %(city_id)s, %(city_name)s, %(lon)s, %(lat)s, %(weather_id)s, %(weather_main)s, %(weather_description)s, %(weather_icon)s, %(temp)s, %(feels_like)s, %(temp_min)s, %(temp_max)s, %(pressure)s, %(humidity)s, %(wind_speed)s, %(wind_deg)s, %(cloud_coverage)s, %(country)s, %(sunrise)s, %(sunset)s, %(gust)s, %(sea_level)s, %(ground_level)s)
                """, parsed_data)
                conn.commit() 
        consumer.commit() # commit offsets after processing each batch

except Exception as e:
    print(f"Error while consuming: {e}")
