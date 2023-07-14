import json
import psycopg2
from utils.kafka_init import create_kafka_consumer
from utils.util import parse_data
from utils.postgres import setup_database


def main():
    conn, cursor = None, None
    try:
        conn, cursor = setup_database()
        consumer = create_kafka_consumer()

        if consumer is None:
            print("Kafka consumer could not be created. Exiting.")
            return

        while True:
            poll_result = consumer.poll(timeout_ms=10000)  # poll for messages, waiting up to 10 seconds
            if not poll_result:  # if the poll result is empty, break the loop
                break
            for tp, messages in poll_result.items():
                for message in messages:
                    print(f"Raw message content: {message.value}")
                    data = json.loads(message.value.decode('utf-8'))
                    parsed_data = parse_data(data)
                    try:
                        # Insert the data into PostgreSQL
                        cursor.execute("""
                        INSERT INTO weather_data (timestamp, timezone, city_id, city_name, lon, lat, weather_id, 
                                    weather_main, weather_description, weather_icon, temp, feels_like, temp_min, 
                                    temp_max, pressure, humidity, wind_speed, wind_deg, cloud_coverage, country, 
                                    sunrise, sunset, gust, sea_level, ground_level) 
                        VALUES (%(timestamp)s, %(timezone)s, %(city_id)s, %(city_name)s, %(lon)s, %(lat)s, 
                                    %(weather_id)s, %(weather_main)s, %(weather_description)s, %(weather_icon)s, 
                                    %(temp)s, %(feels_like)s, %(temp_min)s, %(temp_max)s, %(pressure)s, %(humidity)s, 
                                    %(wind_speed)s, %(wind_deg)s, %(cloud_coverage)s, %(country)s, %(sunrise)s, 
                                    %(sunset)s, %(gust)s, %(sea_level)s, %(ground_level)s)
                        """, parsed_data)
                        conn.commit()
                    except psycopg2.Error as e:
                        print(f"Database error: {e}")
                        conn.rollback()  # roll back the current transaction if there was an error
            consumer.commit() # commit offsets after processing each batch

    except Exception as e:
        print(f"Error while consuming: {e}")
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    main()