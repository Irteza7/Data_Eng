import os
import configparser

def get_settings():
    """Load settings from the configuration file."""
    # current_directory = os.path.dirname(os.path.realpath(__file__))
    current_directory = os.getcwd()
    file_name = "utils/config.ini"
    file_path = os.path.join(current_directory, file_name)
    print(f"Config file path: {file_path}")  # Add this line
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

if __name__ == "__main__":
    settings = get_settings()