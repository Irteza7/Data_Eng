import psycopg2
from .settings import get_settings

def setup_database():
    # Get the settings
    settings = get_settings()
    params = settings["postgres"]

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

    return conn, cursor
