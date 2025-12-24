# fetch_and_upload_raw

from datetime import datetime
from common import ingestion
from airflow import DAG
from airflow.operators.python import PythonOperator


S3_BUCKEY_NAME = "weather"


with DAG(
    "weather_ingestion",
    schedule="@hourly",
    start_date=datetime(2025, 12, 24),
    catchup=False,
    render_template_as_native_obj=True
) as dag:

    get_target_cities = PythonOperator(
        task_id="get_target_cities",
        python_callable=ingestion.get_city_list_from_dw
    )

    ingest_weather_to_minio = PythonOperator(
        task_id="ingest_weather_to_minio",
        python_callable=ingestion.run_weather_ingestion_batch,
        op_kwargs={
            "cities": "{{ ti.xcom_pull(task_ids='get_target_cities') }}",
            "bucket_name": S3_BUCKEY_NAME
        }
    )

    (
        get_target_cities
        >> ingest_weather_to_minio
    )