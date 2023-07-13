from src.api_call import fetch_weather_data
from src.kafka_init import create_kafka_producer
from src.settings import get_settings

# Get the settings
settings = get_settings()
cities = settings["cities"]

def main():
    producer = create_kafka_producer()

    if producer is None:
        print("Kafka producer could not be created. Exiting.")
        return

    for city in cities:
        try:
            print(f"Fetching data for city: {city}")
            data = fetch_weather_data(city)
            print("Data fetched successfully.")
            if data is not None:
                print(f"Sending data to topic for city: {city}")
                producer.send('weather', data)
                print("Data sent successfully.")
                producer.flush()
        except Exception as e:
            print(f"Error occurred for city {city}, type: {type(e)}, error: {str(e)}")

if __name__ == "__main__":
    main()

