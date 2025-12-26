"""
날씨 데이터 변환 모듈

MinIO에 저장된 개별 도시별 JSON 파일들을 읽어와서 데이터 평탄화 및 통합 작업을 수행한 후,
분석 및 적재에 최적화된 Parquet 형식으로 변환하여 다시 MinIO에 저장함
"""


import json
import logging
import io
from datetime import datetime
from typing import Optional

import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# Airflow Task 로그와 연동
logger = logging.getLogger("airflow.task")


def transform_json_to_parquet(bucket_name:str, partition_path:str, execution_date:datetime) -> str:
    """
    MinIO의 Raw 폴더에서 JSON 파일들을 읽어 단일 Parquet 파일로 변환 및 통합

    Args:
        bucket_name (str): MinIO 버킷 이름
        partition_path (str): 읽어올 JSON 파일들이 위치한 S3 prefix 경로
        execution_date (datetime): 파티션 및 파일명 생성을 위한 기준 시각

    Returns:
        str: 생성된 Parquet 파일의 S3 Key (경로). 파일이 없을 경우 빈 문자열 반환
    """
    s3_hook = S3Hook(aws_conn_id="minio_s3")
    
    keys = s3_hook.list_keys(bucket_name=bucket_name, prefix=partition_path)
    if not keys:
        logger.info(f"No files found in {partition_path}")
        return ""

    logger.info(f"Starting transformation for {len(keys)} files in {partition_path}")

    all_data = []
    for key in keys:
        content = s3_hook.read_key(key, bucket_name)
        # json 평탄화
        raw_json = json.loads(content)
        
        flat_data = {
            "city_id": raw_json.get("dw_city_id"),
            "dt": datetime.fromtimestamp(raw_json["dt"]),
            "timezone": raw_json.get("timezone"),
            "weather_id": raw_json["weather"][0]["id"],
            "weather_main": raw_json["weather"][0]["main"],
            "weather_description": raw_json["weather"][0]["description"],
            "weather_icon": raw_json["weather"][0]["icon"],
            "temp": raw_json["main"].get("temp"),
            "feels_like_temp": raw_json["main"].get("feels_like"),
            "temp_min": raw_json["main"].get("temp_min"),
            "temp_max": raw_json["main"].get("temp_max"),
            "pressure": raw_json["main"].get("pressure"),
            "humidity": raw_json["main"].get("humidity"),
            "sea_level_pressure": raw_json["main"].get("sea_level"),
            "grnd_level_pressure": raw_json["main"].get("grnd_level"),
            "visibility": raw_json.get("visibility"),
            "wind_speed": raw_json["wind"].get("speed"),
            "wind_deg": raw_json["wind"].get("deg"),
            "wind_gust": raw_json["wind"].get("gust"),
            "clouds": raw_json["clouds"].get("all"),
            "rain": raw_json.get("rain", {}).get("1h", 0),
            "snow": raw_json.get("snow", {}).get("1h", 0)
        }
        all_data.append(flat_data)

    df = pd.DataFrame(all_data)
    
    partition = (
        f"year={execution_date.year}/"
        f"month={execution_date.month:02d}/"
        f"day={execution_date.day:02d}/"
        f"hour={execution_date.hour:02d}"
    )
    target_key = f"source=openweathermap/type=processed/{partition}/weather_batch.parquet"
    
    parquet_buffer = df.to_parquet(index=False)
    s3_hook.load_bytes(
        bytes_data=parquet_buffer,
        key=target_key,
        bucket_name=bucket_name,
        replace=True
    )
    
    logger.info(f"Successfully transformed {len(all_data)} files to {target_key}")
    return target_key


