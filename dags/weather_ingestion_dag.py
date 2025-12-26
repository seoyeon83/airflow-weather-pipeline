"""
날씨 데이터 수집을 위한 DAG 모듈

이 DAG는 DW에서 수집 대상 도시 목록을 가져온 후, 
OpenWeatherMap API를 통해 데이터를 수집하여 MinIO 데이터 레이크에 적재함
"""


from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from common import ingestion


S3_BUCKET_NAME = "weather"


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
            "bucket_name": S3_BUCKET_NAME,
            "execution_date": "{{ logical_date }}"
        }
    )

    trigger_processing = TriggerDagRunOperator(
        task_id='trigger_processing',
        trigger_dag_id='weather_processing',
        conf={
            'year': '{{ execution_date.year }}',
            'month': '{{ execution_date.month }}',
            'day': '{{ execution_date.day }}',
            'hour': '{{ execution_date.hour }}'
        }
    )

    get_target_cities >> ingest_weather_to_minio >> trigger_processing