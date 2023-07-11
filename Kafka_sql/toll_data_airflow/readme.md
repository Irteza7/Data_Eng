# Apache Airflow ETL for Toll Data

This repository contains an Apache Airflow DAG that automates a data extraction, transformation, and loading (ETL) process for toll data. The ETL process involves extracting data from multiple types of files, consolidating it, and then transforming it.

## Getting Started

Make sure you have Apache Airflow installed. If not, you can install it using pip:

    pip install apache-airflow

Place the DAG python script in your Airflow DAGs folder. By default, this is usually at ~/airflow/dags/.

## Description of the DAG

The Airflow DAG is named ETL_toll_data and it is scheduled to run every 10 minutes. The DAG includes the following tasks, which are executed in order:

    unzip_data: This task unzips a data file.

    extract_data_from_csv: This task extracts certain fields from a CSV file.

    extract_data_from_tsv: This task extracts certain fields from a TSV file.

    extract_data_from_fixed_width: This task extracts certain fields from a fixed-width file.

    consolidate_data: This task consolidates the data extracted from the previous tasks into a single CSV file.

    transform_data: This task transforms the consolidated data by making all characters in the fourth column uppercase.

The DAG uses the BashOperator to execute bash commands for each task.

## Running the DAG

Start the Airflow webserver:

    airflow webserver

In a separate terminal, start the Airflow scheduler:

    airflow scheduler

Access the Airflow UI by navigating to http://localhost:8080 in your web browser.

You should see the ETL_toll_data DAG listed on the dashboard. Click on the play button to trigger the DAG manually.

Remember to monitor the tasks and ensure they are completing successfully. If any issues arise, you can view the logs for each task to debug.

## Troubleshooting

If the tasks fail, check the log output. Make sure that the paths to the data files are correct and that the files are in the expected format.

If you encounter any other issues, refer to the Airflow documentation for guidance.

Please note that this DAG is a basic example and may need to be adapted to suit your specific use case.
