ALTER TABLE latest_station_readings DROP COLUMN IF EXISTS battery_percent_estimate;
ALTER TABLE readings DROP COLUMN IF EXISTS battery_percent_estimate;
