ALTER TABLE readings ADD COLUMN battery_percent_estimate NUMERIC(5,2);
ALTER TABLE latest_station_readings ADD COLUMN battery_percent_estimate NUMERIC(5,2);
