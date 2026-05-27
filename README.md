# Weather Station Data Pipeline

Go-based aggregation backend — the data collection layer of a hyper-localized weather prediction system. Receives batched sensor observations from ESP32 weather stations over MQTT, persists them to PostgreSQL, and maintains a per-station latest-reading snapshot for downstream consumers.

## Overview

Each weather station publishes batches of temperature, humidity, and barometric pressure readings to an MQTT broker at regular intervals. This pipeline subscribes to those batches, fans them out to independent consumers, and stores the timeseries in a PostgreSQL database. The collected data feeds an ML pipeline for local weather forecasting.

This repository contains only the aggregation backend. The sensor firmware and ML training pipeline are separate components of the broader system.

## Getting Started

### Prerequisites

- [Docker](https://www.docker.com/) and Docker Compose

### Run the Stack

```bash
docker compose up --build
```

This starts Mosquitto, PostgreSQL, runs the database migration, and launches both subscribers. The `subscriber-db` service waits for the migration to complete before connecting.

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `MQTT_BROKER` | `tcp://localhost:1883` | MQTT broker address |
| `DATABASE_URL` | `postgres://weather:weather@localhost:5432/weather?sslmode=disable` | PostgreSQL connection string |

Both subscribers retry the broker connection automatically on startup and after disconnection.

### Run Tests

Each service has its own test suite runnable without Docker:

```bash
# subscriber-logger (pure unit tests)
cd subscriber-logger && go test -v ./...

# subscriber-db (integration tests — spins up a real Postgres via testcontainers)
cd subscriber-db && go test -v ./...
```

The `subscriber-db` tests apply the real migration against a throwaway Postgres container and exercise the handler directly — no mocked database.

## Architecture

### Data Flow

```
Weather Stations (ESP32)
   │
   ▼ MQTT publish (observation batches)
MQTT Broker (Mosquitto)
   │
   ├──► subscriber-logger  ──► stdout
   │
   └──► subscriber-db  ──► PostgreSQL
```

Stations publish to the topic `weather/<station_id>/telemetry`. Both subscribers listen on `weather/+/telemetry` (MQTT single-level wildcard), so adding a new station requires no pipeline changes.

### Observation Payload

```json
{
  "station_id": "station_001",
  "sent_at": "2023-11-14T16:01:00Z",
  "samples": [
    {
      "ts": "2023-11-14T16:00:00Z",
      "temperature_c": 21.52,
      "humidity_pct": 48.10,
      "pressure_hpa": 1012.32
    }
  ]
}
```

All timestamps are UTC ISO 8601. Temperatures are stored in Fahrenheit (converted from °C on ingest). The raw JSON payload is preserved in the `raw_payload` column.

### Key Components

| Component | Description |
|-----------|-------------|
| `subscriber-logger` | Subscribes to all telemetry topics and prints each sample to stdout — useful for debugging and monitoring |
| `subscriber-db` | Subscribes to all telemetry topics and persists each sample to PostgreSQL within a single transaction per batch |
| Mosquitto | Eclipse Mosquitto MQTT broker; anonymous connections on port 1883 |
| PostgreSQL | Primary data store; schema managed by golang-migrate |

## Database Schema

| Table | Description |
|-------|-------------|
| `stations` | One row per station; auto-created on first message from that station |
| `readings` | Full timeseries — one row per sample; indexed on `(station_id, recorded_at DESC)` |
| `latest_station_readings` | Snapshot of the most recent reading per station; updated only when a newer sample arrives (out-of-order delivery safe) |

## Project Structure

```
weather-station-data-pipeline/
├── subscriber-logger/
│   ├── main.go              # MQTT subscriber — logs samples to stdout
│   └── handler_test.go      # Unit tests for payload parsing
├── subscriber-db/
│   ├── main.go              # MQTT subscriber — persists samples to Postgres
│   └── handler_test.go      # Integration tests (testcontainers)
├── db/
│   └── migrations/
│       └── 000001_initial_schema.up.sql
├── mosquitto/
│   └── mosquitto.conf       # Broker config (anonymous, port 1883)
├── docker-compose.yml       # Mosquitto + Postgres + both subscribers
└── .github/workflows/test.yml
```

## CI

GitHub Actions runs both test suites on every push to any branch. See [`.github/workflows/test.yml`](.github/workflows/test.yml).

## Dependencies

| Library | Purpose |
|---------|---------|
| [paho.mqtt.golang](https://github.com/eclipse/paho.mqtt.golang) | MQTT client (both subscribers) |
| [pgx](https://github.com/jackc/pgx) | PostgreSQL driver and connection pool (`subscriber-db`) |
| [testcontainers-go](https://github.com/testcontainers/testcontainers-go) | Real Postgres containers for integration tests (`subscriber-db`) |
| [golang-migrate](https://github.com/golang-migrate/migrate) | Database schema migrations (run via Docker Compose) |

## Related Components

This backend is one part of a larger hyper-localized weather prediction system:

- **[Weather Station](https://github.com/Lanham-Software-James/WeatherStation)** — sensor firmware for ESP32; publishes observation batches over MQTT
- **Aggregation Backend** ← you are here — collects and stores observations from all stations
- **ML Pipeline** — trains forecasting models on the aggregated multi-station dataset
