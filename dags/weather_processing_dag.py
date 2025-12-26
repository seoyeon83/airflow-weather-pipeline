"""
날씨 데이터 가공 및 DW 적재 DAG

이 DAG는 Ingestion DAG로부터 실행 신호를 받아 다음 과정을 수행함:
1. MinIO의 Raw 레이어에 저장된 시간대별 JSON 데이터들을 읽어옴
2. 읽어온 여러 도시의 데이터를 병합 및 평탄화하여 Parquet 형식으로 변환
3. 가공된 Parquet 데이터를 DW의 mart.weather 테이블에 Delete-Insert 방식으로 적재
"""


from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from common import transform, load


S3_BUCKET_NAME = "weather"


with DAG(
    "weather_processing",
    schedule=None,
    start_date=datetime(2025, 12, 24),
    catchup=False,
    render_template_as_native_obj=True
) as dag:

    transform_json_to_parquet = PythonOperator(
        task_id="transform_json_to_parquet",
        python_callable=transform.transform_json_to_parquet,
        op_kwargs={
            "bucket_name": S3_BUCKET_NAME,
            "partition_path": (
                "source=openweathermap/type=current/"
                "year={{ dag_run.conf['year'] }}/"
                "month={{ dag_run.conf['month'] }}/"
                "day={{ dag_run.conf['day'] }}/"
                "hour={{ dag_run.conf['hour'] }}/"
            ),
            "execution_date": "{{ execution_date }}"
        }
    )

    load_parquet_to_dw = PythonOperator(
        task_id="load_parquet_to_dw",
        python_callable=load.load_parquet_to_dw,
        op_kwargs={
            "bucket_name": S3_BUCKET_NAME,
            "parquet_key": "{{ ti.xcom_pull(task_ids='transform_json_to_parquet') }}"
        }
    )

    transform_json_to_parquet >> load_parquet_to_dw