CREATE TABLE stations (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  location_name TEXT,
  is_active BOOLEAN NOT NULL DEFAULT true,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE readings (
  id BIGSERIAL PRIMARY KEY,
  station_id TEXT NOT NULL REFERENCES stations(id),
  recorded_at TIMESTAMPTZ NOT NULL,
  received_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  temperature_f NUMERIC(6,2),
  humidity_percent NUMERIC(5,2),
  pressure_hpa NUMERIC(7,2),
  battery_v NUMERIC(5,2),
  rssi_dbm INTEGER,

  raw_payload JSONB NOT NULL
);

CREATE INDEX idx_readings_station_time
ON readings (station_id, recorded_at DESC);

CREATE TABLE latest_station_readings (
  station_id TEXT PRIMARY KEY REFERENCES stations(id),
  reading_id BIGINT NOT NULL REFERENCES readings(id),
  recorded_at TIMESTAMPTZ NOT NULL,
  received_at TIMESTAMPTZ NOT NULL,

  temperature_f NUMERIC(6,2),
  humidity_percent NUMERIC(5,2),
  pressure_hpa NUMERIC(7,2),
  battery_v NUMERIC(5,2),
  rssi_dbm INTEGER,

  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
