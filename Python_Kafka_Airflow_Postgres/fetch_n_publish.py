import os
import requests
import json
import configparser
from kafka import KafkaProducer, KafkaConsumer

current_directory = os.getcwd()
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))


file_name = "Python_Kafka_Airflow_Postgres/config.ini"
file_path = os.path.join(current_directory, file_name)
config = configparser.ConfigParser()
config.read(file_path)

api_key = config.get('API', 'key')
city_id = '2934246'
Params = {'appid': api_key , 'id':city_id}
response = requests.get('http://api.openweathermap.org/data/2.5/weather',params=Params)
data = response.json()

producer.send('weather_topic', data)
producer.flush()
producer.close()