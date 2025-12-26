"""
데이터 적재 모듈

MinIO에 저장된 가공된 Parquet 파일을 읽어와서 DW(PostgreSQL)의 mart.weather 테이블에 적재함
재실행 시 데이터 중복을 방지하기 위해 Delete-Insert 전략을 사용함
"""

import io
import logging
import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Airflow Task 로그와 연동
logger = logging.getLogger("airflow.task")


def load_parquet_to_dw(bucket_name: str, parquet_key: str):
    """
    Parquet 파일을 읽어 PostgreSQL에 Delete-Insert 방식으로 적재함

    전달받은 Parquet 파일 내의 도시 ID와 시간 범위를 추출하여,
    기존에 적재된 동일 범위의 데이터를 삭제한 후 새로운 데이터를 삽입함

    Args:
        bucket_name (str): Parquet 파일이 저장된 MinIO 버킷 이름
        parquet_key (str): 적재할 Parquet 파일의 S3 Key (경로)
    """
    if not parquet_key:
        return

    s3_hook = S3Hook(aws_conn_id="minio_s3")
    pg_hook = PostgresHook(postgres_conn_id="postgres")
    
    logger.info(f"Reading Parquet file from s3://{bucket_name}/{parquet_key}")

    file_obj = s3_hook.get_key(parquet_key, bucket_name)
    df = pd.read_parquet(io.BytesIO(file_obj.get()['Body'].read()))

    # 기존 데이터 삭제
    city_ids = tuple(df['city_id'].unique())
    city_ids_str = f"({city_ids[0]})" if len(city_ids) == 1 else str(city_ids)
    
    min_dt = df['dt'].min()
    max_dt = df['dt'].max()

    delete_sql = """
        DELETE FROM mart.weather 
        WHERE city_id = ANY(%s) 
        AND dt BETWEEN %s AND %s;
    """
    pg_hook.run(delete_sql, parameters=(city_ids, min_dt, max_dt))

    logger.info(f"Deleted existing records for cities {city_ids_str} between {min_dt} and {max_dt}")
    logger.info("Inserting new records into mart.weather table")

    engine = pg_hook.get_sqlalchemy_engine()
    df.to_sql(
        name='weather',
        schema='mart',
        con=engine,
        if_exists='append',
        index=False,
        method='multi' # 멀티 인서트
    )
    logger.info(f"Successfully loaded {len(df)} rows to mart.weather")