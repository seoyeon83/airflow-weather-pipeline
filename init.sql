-- init.sql

-- mart schema 생성
CREATE SCHEMA IF NOT EXISTS mart;

-- city table 생성
CREATE TABLE IF NOT EXISTS mart.city (
    id SERIAL NOT NULL PRIMARY KEY,
    openweather_city_id INTEGER,
    name VARCHAR(100),
    country CHAR(3),
    latitude NUMERIC(10,7),
    longitude NUMERIC(10,7),
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