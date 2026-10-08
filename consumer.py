"""Consumes weather messages from Kafka and writes them into PostgreSQL staging."""

import json

import psycopg2
from kafka import KafkaConsumer

from common import (
    KAFKA_BOOTSTRAP_SERVERS,
    WEATHER_TOPIC,
    PG_HOST,
    PG_PORT,
    PG_DATABASE,
    PG_USER,
    PG_PASSWORD,
)


def get_pg_connection():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD,
    )


def ensure_table(conn):
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS staging;")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS staging.weather_raw (
            id              SERIAL PRIMARY KEY,
            city            VARCHAR(50),
            latitude        NUMERIC(9,4),
            longitude       NUMERIC(9,4),
            temperature     NUMERIC(5,2),
            windspeed       NUMERIC(5,2),
            weathercode     INT,
            observed_at     TIMESTAMP,
            fetched_at      TIMESTAMP
        );
    """)
    conn.commit()
    cur.close()


def insert_record(conn, record):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO staging.weather_raw
            (city, latitude, longitude, temperature, windspeed, weathercode, observed_at, fetched_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """, (
        record.get("city"),
        record.get("latitude"),
        record.get("longitude"),
        record.get("temperature"),
        record.get("windspeed"),
        record.get("weathercode"),
        record.get("observed_at"),
        record.get("fetched_at"),
    ))
    conn.commit()
    cur.close()


def main():
    print("Connecting to PostgreSQL...", flush=True)
    conn = get_pg_connection()
    ensure_table(conn)
    print("PostgreSQL ready. Connecting to Kafka...", flush=True)

    consumer = KafkaConsumer(
        WEATHER_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        group_id="weather-consumer-group",
        api_version=(3, 5, 0),
        api_version_auto_timeout_ms=30000,
        request_timeout_ms=30000,
    )

    print("Consumer started. Listening on topic:", WEATHER_TOPIC, flush=True)
    for message in consumer:
        record = message.value
        try:
            insert_record(conn, record)
            print("Inserted:", record, flush=True)
        except Exception as e:
            print("Error inserting record:", e, flush=True)
            conn.rollback()


if __name__ == "__main__":
    main()
