from kafka import KafkaConsumer
import json
import pyspark
import pandas as pd

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



consumer = KafkaConsumer('weather_topic',
                         bootstrap_servers='localhost:9092',
                         auto_offset_reset='earliest')

df = pd.DataFrame()  # Initialize an empty DataFrame

try:
    while True:
        poll_result = consumer.poll(timeout_ms=5000)  # poll for messages, waiting up to 5 seconds
        if not poll_result:  # if the poll result is empty, break the loop
            break
        for tp, messages in poll_result.items():
            for message in messages:
                data = json.loads(message.value.decode('utf-8'))
                message_df = json_to_df(data)
                df = pd.concat([df, message_df])
                print(f"Consumed message. DataFrame now has {len(df)} row(s).")
except Exception as e:
    print(f"Error while consuming: {e}")


