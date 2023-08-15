from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from schools_etl_parts import (
    download_raw_data,
    save_data_to_azure_data_lake,
    process_raw_data_to_silver,
    transform_silver_data,
)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "email": ["example@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(weeks=1),
}

dag = DAG(
    "schools_etl",
    default_args=default_args,
    description="Extract Transform and Load data from Attom Schools API to S3",
)

download_raw_data = PythonOperator(
    task_id="download_raw_data",
    python_callable=download_raw_data,
    provide_context=True,
    dag=dag,
)

save_raw_data_to_s3 = PythonOperator(
    task_id="save_raw_data_to_s3",
    python_callable=save_data_to_azure_data_lake,
    op_kwargs={
        "layer_name": "raw-data",
        "execution_date": "{{ ds }}",
        "file_format": "json",
    },
    dag=dag,
)

process_raw_data_to_silver = PythonOperator(
    task_id="process_raw_data_to_silver",
    python_callable=process_raw_data_to_silver,
    provide_context=True,
    dag=dag,
)

save_silver_data_to_s3 = PythonOperator(
    task_id="save_silver_data_to_s3",
    python_callable=save_data_to_azure_data_lake,
    op_kwargs={"layer_name": "silver-data", "execution_date": "{{ ds }}"},
    dag=dag,
)

transform_silver_data = PythonOperator(
    task_id="transform_silver_data",
    python_callable=transform_silver_data,
    provide_context=True,
    dag=dag,
)

save_gold_data_to_s3 = PythonOperator(
    task_id="save_gold_data_to_s3",
    python_callable=save_data_to_azure_data_lake,
    op_kwargs={"layer_name": "gold-data", "execution_date": "{{ ds }}"},
    dag=dag,
)

(
    download_raw_data
    >> save_raw_data_to_s3
    >> process_raw_data_to_silver
    >> save_silver_data_to_s3
    >> transform_silver_data
    >> save_gold_data_to_s3
)
