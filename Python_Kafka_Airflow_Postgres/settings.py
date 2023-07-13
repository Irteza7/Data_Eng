import os
import configparser

def get_settings():
    """Load settings from the configuration file."""
    current_directory = os.getcwd()
    file_name = "Python_Kafka_Airflow_Postgres/config.ini"
    file_path = os.path.join(current_directory, file_name)
    config = configparser.ConfigParser()
    config.read(file_path)

    api_key = config.get('API', 'key')
    cities = config.get('API', 'cities').split(',')

    postgres_user = config.get('Postgres', 'POSTGRES_USER')
    postgres_password = config.get('Postgres','POSTGRES_PASSWORD')
    database_name = config.get('Postgres', 'DATABASE_NAME')
    host = config.get('Postgres', 'HOST')
    port = config.get('Postgres', 'PORT')

    # Build the Postgres parameters dictionary
    postgres_params = {
        "database": database_name,
        "host": host,
        "port": port,
        "user": postgres_user,
        "password": postgres_password
    }

    return {
        "api_key": api_key,
        "cities": cities,
        "postgres": postgres_params
    }
