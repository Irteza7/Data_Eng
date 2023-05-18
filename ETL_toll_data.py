from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'Irteza Khan',
    'start_date': days_ago(0),
    'email': ['irteza@somemail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}



# define the DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)


unzip_data = BashOperator(
    task_id='unzip_data'
    bash_command='tar -xvzf /home/irteza/airflow/dags/finalassignment/tolldata.tgz -C /home/irteza/airflow/dags/finalassignment'
    dag=dag
)

extract_data_from_csv = BashOperator(
    task_id = 'extract_data_from_csv'
    bash_command='cut -d"," -f1-4 /home/irteza/airflow/dags/finalassignment/tolldata/vehicle-data.csv > /home/irteza/airflow/dags/finalassignment/tolldata/csv_data.csv'
    dag=dag
)

extract_data_from_tsv = BashOperator(
    task_id = 'extract_data_from_tsv'
    bash_command='cut -f5-7 /home/irteza/airflow/dags/finalassignment/tolldata/tollplaza-data.tsv | tr "\t" "," > /home/irteza/airflow/dags/finalassignment/tolldata/tsv_data.csv'
)

extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width'
    bash_command='awk "NF{print $(NF-1),$NF}" OFS="," /home/irteza/airflow/dags/finalassignment/tolldata/payment-data.txt > /home/irteza/airflow/dags/finalassignment/tolldata/fixed_width_data.csv'
    dag=dag
)

consolidate_data = BashOperator(
    task_id='consolidate_data'
    bash_command='paste /home/irteza/airflow/dags/finalassignment/tolldata/csv_data.csv /home/irteza/airflow/dags/finalassignment/tolldata/tsv_data.csv /home/irteza/airflow/dags/finalassignment/tolldata/fixed_width_data.csv > /home/irteza/airflow/dags/finalassignment/tolldata/extracted_data.csv'
    dag=dag
)

transform_data = BashOperator(
    task_id='transform_data'
    bash_command='awk -F","" -v OFS="," "{ $4 = toupper($4); print }" extracted_data.csv > transformed_data.csv'
    dag=dag
)

unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data