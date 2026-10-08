"""Shared configuration for the weather streaming pipeline."""

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
WEATHER_TOPIC = "weather-raw"

# Cities being tracked (Open-Meteo needs lat/lon, not city names)
CITIES = [
    {"city": "Lahore", "latitude": 31.5497, "longitude": 74.3436},
    {"city": "Karachi", "latitude": 24.8607, "longitude": 67.0011},
    {"city": "Islamabad", "latitude": 33.6844, "longitude": 73.0479},
]

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

# Postgres connection (the same local PostgreSQL used by DBeaver / Airflow)
PG_HOST = "host.docker.internal"
PG_PORT = 5432
PG_DATABASE = "postgres"
PG_USER = "postgres"        # change if your username is different
PG_PASSWORD = "your_password_here"  # change to your actual PostgreSQL password
