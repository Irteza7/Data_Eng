import os
import requests
import json
import configparser
from kafka import KafkaProducer 

current_directory = os.getcwd()
file_name = "Python_Kafka_Airflow_Postgres/config.ini"
file_path = os.path.join(current_directory, file_name)
config = configparser.ConfigParser()
config.read(file_path)

api_key = config.get('API', 'key')

cities = ["London", "New York", "Tokyo", "Paris", "Berlin", "Sydney", "Moscow",
          "Rio de Janeiro", "Cape Town", "Beijing", "Bangkok", "Toronto", "Mexico City",
          "Karachi", "Seoul", "Istanbul", "Mumbai", "Zurich", "Buenos Aires", "Dubai"]

producer = None
try:
    producer = KafkaProducer(bootstrap_servers='localhost:9092',
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    for city in cities:
        Params = {'appid': api_key , 'q': city, 'units': 'metric'}

        response = requests.get('http://api.openweathermap.org/data/2.5/weather',params=Params) #can change to forecast 
        data = response.json()

        if response.status_code != 200:
            raise Exception(f"Failed to fetch data from API for city {city}: {response.content}")

        producer.send('weather_topic', data)
        producer.flush()
except Exception as e:
    print(f"Error occurred for city {city}: {e}")
finally:
    if producer is not None:
        producer.close()


# print(json.dumps(data, indent=4)) #to check the structure