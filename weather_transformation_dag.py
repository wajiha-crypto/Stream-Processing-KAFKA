from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "retries": 1,
}

TRANSFORM_SQL = """
CREATE SCHEMA IF NOT EXISTS warehouse;

CREATE TABLE IF NOT EXISTS warehouse.dim_city (
    city_id     SERIAL PRIMARY KEY,
    city        VARCHAR(50) UNIQUE,
    latitude    NUMERIC(9,4),
    longitude   NUMERIC(9,4)
);

CREATE TABLE IF NOT EXISTS warehouse.dim_date (
    date_id     BIGINT PRIMARY KEY,
    full_date   DATE,
    hour        INT,
    day         INT,
    month       INT,
    year        INT
);

CREATE TABLE IF NOT EXISTS warehouse.fact_weather_hourly (
    id              SERIAL PRIMARY KEY,
    city_id         INT REFERENCES warehouse.dim_city(city_id),
    date_id         BIGINT REFERENCES warehouse.dim_date(date_id),
    avg_temperature NUMERIC(5,2),
    max_temperature NUMERIC(5,2),
    min_temperature NUMERIC(5,2),
    avg_windspeed   NUMERIC(5,2),
    reading_count   INT,
    UNIQUE (city_id, date_id)
);

INSERT INTO warehouse.dim_city (city, latitude, longitude)
SELECT DISTINCT city, latitude, longitude
FROM staging.weather_raw
WHERE fetched_at >= NOW() - INTERVAL '1 hour'
ON CONFLICT (city) DO NOTHING;

INSERT INTO warehouse.dim_date (date_id, full_date, hour, day, month, year)
SELECT
    TO_CHAR(DATE_TRUNC('hour', NOW()), 'YYYYMMDDHH24')::BIGINT,
    DATE_TRUNC('hour', NOW())::DATE,
    EXTRACT(HOUR FROM NOW()),
    EXTRACT(DAY FROM NOW()),
    EXTRACT(MONTH FROM NOW()),
    EXTRACT(YEAR FROM NOW())
ON CONFLICT (date_id) DO NOTHING;

INSERT INTO warehouse.fact_weather_hourly
    (city_id, date_id, avg_temperature, max_temperature, min_temperature, avg_windspeed, reading_count)
SELECT
    dc.city_id,
    TO_CHAR(DATE_TRUNC('hour', NOW()), 'YYYYMMDDHH24')::BIGINT AS date_id,
    AVG(w.temperature),
    MAX(w.temperature),
    MIN(w.temperature),
    AVG(w.windspeed),
    COUNT(*)
FROM staging.weather_raw w
JOIN warehouse.dim_city dc ON dc.city = w.city
WHERE w.fetched_at >= NOW() - INTERVAL '1 hour'
GROUP BY dc.city_id
ON CONFLICT (city_id, date_id) DO UPDATE SET
    avg_temperature = EXCLUDED.avg_temperature,
    max_temperature = EXCLUDED.max_temperature,
    min_temperature = EXCLUDED.min_temperature,
    avg_windspeed   = EXCLUDED.avg_windspeed,
    reading_count   = EXCLUDED.reading_count;
"""

with DAG(
    dag_id="weather_transformation_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
) as dag:

    transform_to_warehouse = PostgresOperator(
        task_id="transform_weather_to_warehouse",
        postgres_conn_id="postgres_default",
        sql=TRANSFORM_SQL,
    )
