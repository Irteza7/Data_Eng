from kafka import KafkaProducer, KafkaConsumer
import requests
import json

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

api_key = 'f9361d4590585fa8de005eca48935618'
city_id = '2934246'
Params = {'appid': api_key , 'id':city_id}
response = requests.get('http://api.openweathermap.org/data/2.5/weather',params=Params)
data = response.json()


# data_json = json.dumps(data).encode('utf-8')
# producer.send('bankbranch', data_json)


producer.send('weather_topic', data)
producer.flush()
producer.close()


# consumer = KafkaConsumer('bankbranch',
#                          bootstrap_servers='localhost:9092',
#                          auto_offset_reset='earliest')

# for message in consumer:
#     print(message.value)
