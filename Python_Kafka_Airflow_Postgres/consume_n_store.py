from kafka import KafkaConsumer
import json
import pyspark

# consumer = KafkaConsumer('weather_topic',
#                          bootstrap_servers='localhost:9092',
#                          auto_offset_reset='earliest',
#                          value_deserializer=lambda m: json.loads(m.decode('utf-8')))

consumer = KafkaConsumer('bankbranch',
                         bootstrap_servers='localhost:9092',
                         auto_offset_reset='earliest')

try:
    consumer.max_poll_records = 1  # Limit to fetching one message at a time
    for message in consumer:
        print(message.value)
        # data1 = message.value
        
except Exception as e:
    print(f"Error while consuming: {e}")


    # process data and store in database

