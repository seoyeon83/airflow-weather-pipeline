"""
OpenWeatherMap API를 통한 날씨 데이터 수집 및 MinIO 적재 모듈

이 모듈은 DW(PostgreSQL)에서 수집 대상 도시 목록을 가져오고, 
OpenWeatherMap API를 호출하여 날씨 데이터를 수집한 후, 
MinIO(S3 호환 스토리지)의 데이터 레이크 레이어에 Raw JSON 파일을 적재하는 기능을 수행함
"""

import os
import requests
import json
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any

# Airflow Task 로그와 연동
logger = logging.getLogger("airflow.task")


def get_city_list_from_dw() -> List[Dict[str, Any]]:
    """
    DW의 mart.city 테이블에서 수집 대상 도시 정보를 조회

    Returns:
        List[Dict[str, Any]]: 도시 ID, 위도, 경도, 이름을 포함하는 딕셔너리 리스트
    Raises:
        AirflowException: 데이터베이스 연결 실패 또는 쿼리 실행 오류 발생 시
    """
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    
    logger.info("Fetching target city list from PostgreSQL...")

    hook = PostgresHook(postgres_conn_id="postgres")
    sql = "SELECT id, latitude::float, longitude::float, name FROM mart.city"
    
    records = hook.get_records(sql)
    city_list = [
        {"id": r[0], "lat": r[1], "lon": r[2], "name": r[3]}
        for r in records
    ]
    
    logger.info(f"Successfully fetched {len(city_list)} cities from DW")
    return city_list


def fetch_current_weather(lat: float, lon: float) -> Dict[str, Any]:
    """
    OpenWeatherMap API를 호출하여 특정 좌표의 현재 날씨 데이터를 수집

    Args:
        lat (float): 위도
        lon (float): 경도
    Returns:
        Dict[str, Any]: API 응답 결과 JSON 데이터
    Raises:
        ValueError: API 키가 환경 변수에 존재하지 않을 경우
        Exception: API 응답 상태 코드가 200이 아닐 경우
    """
    api_key = os.getenv("OPENWEATHER_API_KEY")
    if not api_key:
        logger.error("Environment variable 'OPENWEATHER_API_KEY' is missing")
        raise ValueError("OPENWEATHER_API_KEY is not exist")

    api_url = (
        f"https://api.openweathermap.org/data/2.5/weather?"
        f"lat={lat}&lon={lon}&appid={api_key}&units=metric"
    )
    
    response = requests.get(api_url)

    if response.status_code == 200:
        return response.json()
    
    error_msg = f"API call failed (Status: {response.status_code}): {response.text}"
    logger.error(error_msg)
    raise Exception(error_msg)


def upload_raw_to_minio(data: Dict[str, Any], bucket_name: str, execution_date: datetime) -> str:
    """
    수집된 날씨 JSON 데이터를 MinIO의 지정된 경로에 업로드

    Args:
        data (Dict[str, Any]): 수집된 날씨 데이터
        bucket_name (str): 업로드할 MinIO 버킷 이름
    Returns:
        str: 업로드된 파일의 S3 Key 경로
    Note:
        - 경로 구조: source=openweathermap/type=current/year=YYYY/month=MM/day=DD/hour=HH 
        - 파일명 구조: {unix_timestamp}_{city_name}.json
    """
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    dt = datetime.fromtimestamp(data["dt"], tz=timezone.utc)
    city = data.get("name", "unknown").replace(" ", "_").lower()

    partition = (
        f"year={execution_date.year}/"
        f"month={execution_date.month:02d}/"
        f"day={execution_date.day:02d}/"
        f"hour={execution_date.hour:02d}"
    )
    filename = f"{data['dt']}_{city}.json"
    s3_key = f"source=openweathermap/type=current/{partition}/{filename}"
    
    logger.info(f"Attempting to upload raw data to s3://{bucket_name}/{s3_key}")

    s3_hook = S3Hook(aws_conn_id="minio_s3")
    s3_hook.load_string(
        string_data=json.dumps(data, ensure_ascii=False),
        key=s3_key,
        bucket_name=bucket_name,
        replace=True
    )

    logger.info(f"Upload completed: {s3_key}")
    return s3_key


def run_weather_ingestion_batch(cities: List[Dict[str, Any]], bucket_name: str) -> List[str]:
    """
    도시 목록을 순회하며 날씨 데이터를 일괄 수집 및 적재

    Args:
        cities (List[Dict[str, Any]]): 수집 대상 도시 정보 리스트
        bucket_name (str): 목적지 버킷 이름
    Returns:
        List[str]: 업로드 성공한 파일들의 S3 Key 목록
    """
    paths = []
    success_count = 0
    fail_count = 0

    logger.info(f"Starting ingestion batch for {len(cities)} cities")

    for city in cities:
        try:
            data = fetch_current_weather(city["lat"], city["lon"])
            
            data["dw_city_id"] = city["id"]

            key = upload_raw_to_minio(data, bucket_name)
            paths.append(key)
            success_count += 1

        except Exception as e:
            fail_count += 1
            logger.warning(f"Failed to ingest weather for city '{city.get('name')}': {str(e)}")
            continue
            
    logger.info(f"Batch ingestion finished. Success: {success_count}, Failure: {fail_count}")
    return paths