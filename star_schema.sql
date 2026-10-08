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
