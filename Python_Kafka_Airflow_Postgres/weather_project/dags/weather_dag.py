import sys
sys.path.insert(0, "/home/irteza/Data eng/Python_Kafka_Airflow_Postgres/weather_project")

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from fetch_n_publish import main as fetch_n_publish_main
from consume_n_store import main as consume_n_store_main

# Define the default args for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 7, 14),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),

}

# Instantiate the DAG
with DAG('weather_pipeline', default_args=default_args, schedule_interval=timedelta(hours=4),catchup=False) as dag:

    # Define the tasks
    fetch_n_publish_task = PythonOperator(
        task_id='fetch_n_publish',
        python_callable=fetch_n_publish_main,
    )

    consume_n_store_task = PythonOperator(
        task_id='consume_n_store',
        python_callable=consume_n_store_main,
    )

    # Set the task dependencies
    fetch_n_publish_task >> consume_n_store_task

