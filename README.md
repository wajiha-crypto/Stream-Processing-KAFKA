# Stream-Processing-KAFKA — Real-Time Weather Pipeline

Real-time weather data pipeline: Open-Meteo API → Kafka → PostgreSQL staging → hourly Airflow warehouse transformation (star schema).

## Architecture

```
Open-Meteo API (live weather)
      │
      ▼
producer.py  ──publishes──►  Kafka topic: weather-raw
      │
      ▼
consumer.py  ──writes──►  staging.weather_raw (PostgreSQL)
      │
      ▼
Airflow DAG (hourly)  ──transforms──►  warehouse star schema
                                        (dim_city, dim_date, fact_weather_hourly)
```

## Files

- `docker-compose.yml` — Kafka, Zookeeper, producer, and consumer containers
- `common.py` — shared config: tracked cities, Kafka topic, PostgreSQL connection
- `producer.py` — polls Open-Meteo every 60s for each city, publishes to Kafka
- `consumer.py` — consumes from Kafka, writes into `staging.weather_raw`
- `sql/star_schema.sql` — warehouse star schema definition
- `dags/weather_transformation_dag.py` — hourly Airflow DAG that builds the warehouse
  from staging data (runs in the same Airflow instance as the `DAG_REST_API` project)

## How to run

1. Edit `common.py` and set `PG_USER` / `PG_PASSWORD` to your local PostgreSQL credentials.
2. Run:
   ```
   docker compose up -d
   ```
3. Copy `dags/weather_transformation_dag.py` into the existing Airflow project's `dags/`
   folder (this reuses the Airflow instance set up for `DAG_REST_API`, the same way the
   original pipeline reused Airflow for hourly warehouse builds).
4. In Airflow (`localhost:8080`), unpause and trigger `weather_transformation_pipeline`.

## Verify

```sql
SELECT COUNT(*) FROM staging.weather_raw;
SELECT * FROM warehouse.fact_weather_hourly;
```

## Notes

- Tracks three cities: Lahore, Karachi, Islamabad.
- `kafka-python` needed `api_version=(3, 5, 0)` set explicitly to connect reliably to
  `confluentinc/cp-kafka:7.5.0` — without it, the client would hang indefinitely on connect.
- If Kafka/Zookeeper ever fail to start with a `NodeExistsException`, run
  `docker compose down -v` to clear stale broker state, then `docker compose up -d` again.

## Status

- **Verified working:** Open-Meteo → Kafka → PostgreSQL (`staging.weather_raw` reached 586+ rows with live readings for Lahore, Karachi, Islamabad).
- **Partially verified:** the hourly Airflow DAG (`weather_transformation_pipeline`) completed successfully on a manual trigger, but later scheduled runs were failing and the root cause was not identified before wrapping up. Check the task log in the Airflow UI if you reproduce this.
- **Not done yet:** Tableau dashboard (Tableau Desktop was not installed).
