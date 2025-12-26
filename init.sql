-- init.sql

-- mart schema 생성
CREATE SCHEMA IF NOT EXISTS mart;

-- city table 생성
CREATE TABLE IF NOT EXISTS mart.city (
    id SERIAL NOT NULL PRIMARY KEY,
    openweather_city_id INTEGER NOT NULL UNIQUE,
    name VARCHAR(100) NOT NULL,
    country CHAR(3) NOT NULL,
    latitude NUMERIC(10,7) NOT NULL,
    longitude NUMERIC(10,7) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- 데이터 삽입
INSERT INTO mart.city (openweather_city_id, name, country, latitude, longitude) VALUES
(2968815, 'paris', 'FRA', 48.8566, 2.3522),
(6545095, 'madrid', 'ESP', 40.4168, -3.7038),
(1857654, 'tokyo', 'JPN', 35.6769, 139.7639),
(6545158, 'rome', 'ITA', 41.9028, 12.4964),
(3173435, 'milan', 'ITA', 45.4642, 9.19),
(5128581, 'newyork', 'USA', 40.7128, -74.006),
(2759794, 'amsterdam', 'NLD', 52.3676, 4.9041),
(3119123, 'barcelona', 'ESP', 41.3829, 2.1774),
(1880252, 'singapore', 'SGP', 1.2899, 103.8519),
(1835848, 'seoul', 'KOR', 37.5665, 126.978),
(1608132, 'bangkok', 'THA', 13.7525, 100.4935),
(8223932, 'hongkong', 'HKG', 22.2793, 114.1628),
(2643743, 'london', 'GBR', 51.5074, -0.1278),
(1821274, 'macao', 'MAC', 22.1987, 113.5439),
(738354, 'istanbul', 'TUR', 41.0091, 28.97),
(290845, 'dubai', 'ARE', 25.2653, 55.2925),
(104515, 'mecca', 'SAU', 21.4208, 39.8269),
(323777, 'antalya', 'TUR', 36.8969, 30.7133),
(1733046, 'kualalumpur', 'MYS', 3.139, 101.6869),
(1838519, 'busan', 'KOR', 35.1796, 129.0756);


-- weather table 생성
CREATE TABLE IF NOT EXISTS mart.weather (
    id BIGSERIAL PRIMARY KEY,                          -- 날씨 데이터 ID (자동 증가)
    city_id INTEGER NOT NULL,                         -- mart.city 테이블 참조 ID
    dt TIMESTAMPTZ NOT NULL,                          -- 관측 시간 (Unix Timestamp 변환 결과)
    timezone INTEGER NOT NULL,                        -- UTC 오프셋 (초 단위)
    
    -- 기상 상태 정보
    weather_id INTEGER,                               -- 기상 상태 ID (예: 501)
    weather_main VARCHAR(50),                         -- 날씨 그룹 (예: Rain)
    weather_description TEXT,                         -- 날씨 상세 설명
    weather_icon VARCHAR(10),                         -- 아이콘 코드 (예: 10d)
    
    -- 온도 정보 (Celsius)
    temp NUMERIC(5,2),                                -- 현재 기온
    feels_like_temp NUMERIC(5,2),                     -- 체감 기온
    temp_min NUMERIC(5,2),                            -- 최저 기온
    temp_max NUMERIC(5,2),                            -- 최고 기온
    
    -- 대기 정보
    pressure INTEGER,                                 -- 대기압 (hPa)
    humidity INTEGER,                                 -- 습도 (%)
    sea_level_pressure INTEGER,                       -- 해수면 기압 (hPa)
    grnd_level_pressure INTEGER,                      -- 지표면 기압 (hPa)
    visibility INTEGER,                               -- 가시거리 (m)
    
    -- 바람 정보
    wind_speed NUMERIC(5,2),                          -- 풍속 (m/s)
    wind_deg INTEGER,                                 -- 풍향 (도)
    wind_gust NUMERIC(5,2),                           -- 돌풍 (m/s)
    
    -- 구름 및 강수량 정보
    clouds INTEGER,                                   -- 구름 양 (%)
    rain NUMERIC(6,2) DEFAULT 0,                      -- 1시간 강수량 (mm)
    snow NUMERIC(6,2) DEFAULT 0,                      -- 1시간 적설량 (mm)
    
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,

    -- 외래키 제약 조건 설정
    CONSTRAINT fk_city FOREIGN KEY (city_id) REFERENCES mart.city(id) ON DELETE CASCADE
);

-- 성능 최적화를 위한 인덱스
CREATE INDEX IF NOT EXISTS idx_weather_city_dt ON mart.weather (city_id, dt DESC);