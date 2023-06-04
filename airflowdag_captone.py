from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'irteza',
    'start_date': datetime(2023, 5, 17),
    'email': ['your_email@example.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'process_web_log',
    default_args=default_args,
    description='Process web log data',
    schedule_interval=timedelta(days=1),
)

# Task to extract data
t1 = BashOperator(
    task_id='extract_data',
    bash_command="""
    grep -oE '[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+' ~/airflow/dags/capstone/accesslog.txt > ~/airflow/dags/capstone/extracted_data.txt
    """,
    dag=dag,
)

# Task to transform data
t2 = BashOperator(
    task_id='transform_data',
    bash_command="""
    grep -v "198.46.149.143" ~/airflow/dags/capstone/extracted_data.txt > ~/airflow/dags/capstone/transformed_data.txt
    """,
    dag=dag,
)

# Task to load data
t3 = BashOperator(
    task_id='load_data',
    bash_command="""
    tar -cvf ~/airflow/dags/capstone/weblog.tar ~/airflow/dags/capstone/transformed_data.txt
    """,
    dag=dag,
)

# Set task dependencies
t1 >> t2 >> t3
