from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

def create_kafka_producer():
    try:
        producer = KafkaProducer(bootstrap_servers='localhost:9092')
        return producer
    except KafkaError as e:
        print(f"Failed to create Kafka Producer: {e}")
        return None

def create_kafka_consumer(group_id='weather_group'):
    try:
        consumer = KafkaConsumer('weather_topic',
                                 bootstrap_servers='localhost:9092',
                                 auto_offset_reset='earliest',
                                 group_id=group_id)
        return consumer
    except KafkaError as e:
        print(f"Failed to create Kafka Consumer: {e}")
        return None
