from utils.api_call import fetch_weather_data
from utils.kafka_init import create_kafka_producer
from utils.settings import get_settings

# Get the settings
settings = get_settings()
cities = settings["cities"]

def main():
    producer = create_kafka_producer()

    if producer is None:
        print("Kafka producer could not be created. Exiting.")
        return
    try: 
        for city in cities:
            try:
                data = fetch_weather_data(city)
                if data is not None:
                    producer.send('weather', data)
                    producer.flush()
                    print(f"{city} weather data sent successfully")
            except Exception as e:
                print(f"Error occurred for city {city}, type: {type(e)}, error: {str(e)}")
    finally:
        if producer is not None:
            producer.close()

if __name__ == "__main__":
    main()

