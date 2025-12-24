'''
openweather에서 api를 호출해 데이터를 수집하는 메소드 모음
'''

import os
import requests
import json
from datetime import datetime, timezone


# DW에서 조회할 도시 정보 가져오기
def get_city_list_from_dw():
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    
    hook = PostgresHook(postgres_conn_id="postgres")
    sql = "SELECT id, latitude::float, longitude::float, name FROM mart.city"
    records = hook.get_records(sql)
    
    return [
        {"id": r[0], "lat": r[1], "lon": r[2], "name": r[3]}
        for r in records
    ]


# openweather current API 호출 및 json 데이터 리턴
def fetch_current_weather(lat, lon):
    api_key = os.getenv("OPENWEATHER_API_KEY")
    if not api_key:
        raise ValueError("OPENWEATHER_API_KEY is not exist")

    api_url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}&units=metric"
    
    response = requests.get(api_url)

    if response.status_code == 200:    
        return response.json()
    
    else:
        error_msg = f"API 호출 실패 (Status: {response.status_code}): {response.text}"
        print(error_msg)
        raise Exception(error_msg)


# json 데이터를 S3 Hook으로 MinIO에 업로드
def upload_raw_to_minio(data, bucket_name):
    dt = datetime.fromtimestamp(data["dt"], tz=timezone.utc)
    city = data.get("name", "unknown").replace(" ", "_").lower()

    partition = f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/hour={dt.hour:02d}"
    filename = f"{data['dt']}_{city}.json"

    s3_key = f"source=openweathermap/type=current/{partition}/{filename}"
    
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    # S3Hook으로 업로드
    s3_hook = S3Hook(aws_conn_id="minio_s3")

    s3_hook.load_string(
        string_data=json.dumps(data, ensure_ascii=False),
        key=s3_key,
        bucket_name=bucket_name,
        replace=True
    )

    return s3_key


# 주어진 city 들의 정보가 담긴 리스트를 기반으로 날씨 데이터를 수집, MinIO에 적재 후 경로명들을 반환
def run_weather_ingestion_batch(cities, bucket_name):
    paths = list()

    
    # 각 도시별로 데이터 수집 및 MinIO에 적재
    for city in cities:
        try:
            data = fetch_current_weather(city["lat"], city["lon"])
            data["dw_city_id"] = city["id"]

            key = upload_raw_to_minio(data, bucket_name)
            paths.append(key)

            print(f"Successfully ingested: {city['name']} -> {key}")

        except Exception as e:
            print(f"Failed to ingest weather for {city['name']}: {str(e)}")
            continue
        
    return paths
