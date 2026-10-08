"""Polls the Open-Meteo API for current weather and publishes to Kafka."""

import json
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer

from common import KAFKA_BOOTSTRAP_SERVERS, WEATHER_TOPIC, CITIES, OPEN_METEO_URL

POLL_INTERVAL_SECONDS = 60


def get_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        api_version=(3, 5, 0),
        api_version_auto_timeout_ms=30000,
        request_timeout_ms=30000,
    )


def fetch_current_weather(city):
    params = {
        "latitude": city["latitude"],
        "longitude": city["longitude"],
        "current_weather": True,
    }
    response = requests.get(OPEN_METEO_URL, params=params, timeout=15)
    response.raise_for_status()
    data = response.json()
    current = data.get("current_weather", {})
    return {
        "city": city["city"],
        "latitude": city["latitude"],
        "longitude": city["longitude"],
        "temperature": current.get("temperature"),
        "windspeed": current.get("windspeed"),
        "weathercode": current.get("weathercode"),
        "observed_at": current.get("time"),
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }


def main():
    print("Connecting to Kafka...", flush=True)
    producer = get_producer()
    print("Producer started. Polling Open-Meteo every", POLL_INTERVAL_SECONDS, "seconds.", flush=True)
    while True:
        for city in CITIES:
            try:
                record = fetch_current_weather(city)
                producer.send(WEATHER_TOPIC, value=record)
                print("Sent:", record, flush=True)
            except Exception as e:
                print("Error fetching/sending for", city["city"], ":", e, flush=True)
        producer.flush()
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
