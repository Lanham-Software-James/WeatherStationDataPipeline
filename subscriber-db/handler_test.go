package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// ---------------------------------------------------------------------------
// Test infrastructure
// ---------------------------------------------------------------------------

var testPool *pgxpool.Pool

// mockMsg satisfies the mqtt.Message interface just enough for handler tests.
type mockMsg struct {
	topic   string
	payload []byte
}

func (m mockMsg) Topic() string     { return m.topic }
func (m mockMsg) Payload() []byte   { return m.payload }
func (m mockMsg) Duplicate() bool   { return false }
func (m mockMsg) Qos() byte         { return 1 }
func (m mockMsg) Retained() bool    { return false }
func (m mockMsg) MessageID() uint16 { return 0 }
func (m mockMsg) Ack()              {}

func TestMain(m *testing.M) {
	ctx := context.Background()

	container, err := tcpostgres.Run(ctx,
		"postgres:16",
		tcpostgres.WithDatabase("weather"),
		tcpostgres.WithUsername("weather"),
		tcpostgres.WithPassword("weather"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second),
		),
	)
	if err != nil {
		panic(fmt.Sprintf("start postgres container: %v", err))
	}

	connStr, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		panic(fmt.Sprintf("get connection string: %v", err))
	}

	testPool, err = pgxpool.New(ctx, connStr)
	if err != nil {
		panic(fmt.Sprintf("create pool: %v", err))
	}

	// Apply the real migration so tests run against the actual schema.
	sql, err := os.ReadFile("../db/migrations/000001_initial_schema.up.sql")
	if err != nil {
		panic(fmt.Sprintf("read migration file: %v", err))
	}
	if _, err = testPool.Exec(ctx, string(sql)); err != nil {
		panic(fmt.Sprintf("apply migration: %v", err))
	}

	code := m.Run()

	testPool.Close()
	_ = container.Terminate(ctx)
	os.Exit(code)
}

func resetDB(t *testing.T) {
	t.Helper()
	_, err := testPool.Exec(context.Background(),
		`TRUNCATE latest_station_readings, readings, stations RESTART IDENTITY CASCADE`)
	if err != nil {
		t.Fatalf("resetDB: %v", err)
	}
}

func batchPayload(t *testing.T, stationID, sentAt string, samples []Observation) []byte {
	t.Helper()
	b, err := json.Marshal(ObservationBatch{StationID: stationID, SentAt: sentAt, Samples: samples})
	if err != nil {
		t.Fatalf("marshal batch: %v", err)
	}
	return b
}

func nowISO() string { return time.Now().UTC().Format(time.RFC3339) }

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// A valid single-sample batch registers the station automatically.
func TestHandleMessage_AutoCreatesStation(t *testing.T) {
	resetDB(t)
	handler := makeHandler(testPool)

	payload := batchPayload(t, "station-000", nowISO(), []Observation{
		{Ts: nowISO(), TempC: 20.0, HumidityPct: 50.0, PressureHPa: 1013.0},
	})
	handler(nil, mockMsg{topic: "weather/station-000/telemetry", payload: payload})

	var count int
	if err := testPool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM stations WHERE id = 'station-000'`).Scan(&count); err != nil {
		t.Fatalf("query stations: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 station row, got %d", count)
	}
}

// A second message for the same station must not create a duplicate station row.
func TestHandleMessage_StationNotDuplicated(t *testing.T) {
	resetDB(t)
	handler := makeHandler(testPool)

	payload := batchPayload(t, "station-000", nowISO(), []Observation{
		{Ts: nowISO(), TempC: 20.0, HumidityPct: 50.0, PressureHPa: 1013.0},
	})
	handler(nil, mockMsg{topic: "weather/station-000/telemetry", payload: payload})
	handler(nil, mockMsg{topic: "weather/station-000/telemetry", payload: payload})

	var count int
	if err := testPool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM stations WHERE id = 'station-000'`).Scan(&count); err != nil {
		t.Fatalf("query stations: %v", err)
	}
	if count != 1 {
		t.Errorf("expected exactly 1 station row after two messages, got %d", count)
	}
}

// A single-sample batch inserts one row into readings.
func TestHandleMessage_InsertsReading(t *testing.T) {
	resetDB(t)
	handler := makeHandler(testPool)

	payload := batchPayload(t, "station-000", nowISO(), []Observation{
		{Ts: "2026-05-26T12:00:00Z", TempC: 20.0, HumidityPct: 55.0, PressureHPa: 1010.0},
	})
	handler(nil, mockMsg{topic: "weather/station-000/telemetry", payload: payload})

	var count int
	if err := testPool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM readings WHERE station_id = 'station-000'`).Scan(&count); err != nil {
		t.Fatalf("query readings: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 reading, got %d", count)
	}
}

// Temperature must be stored converted to Fahrenheit (0 °C → 32 °F).
func TestHandleMessage_TemperatureStoredAsF(t *testing.T) {
	resetDB(t)
	handler := makeHandler(testPool)

	payload := batchPayload(t, "station-000", nowISO(), []Observation{
		{Ts: nowISO(), TempC: 0.0, HumidityPct: 50.0, PressureHPa: 1013.0},
	})
	handler(nil, mockMsg{topic: "weather/station-000/telemetry", payload: payload})

	var tempF float64
	if err := testPool.QueryRow(context.Background(),
		`SELECT temperature_f FROM readings WHERE station_id = 'station-000'`).Scan(&tempF); err != nil {
		t.Fatalf("query temperature_f: %v", err)
	}
	if tempF != 32.0 {
		t.Errorf("expected 0°C → 32°F, got %.2f°F", tempF)
	}
}

// A multi-sample batch inserts one reading row per sample.
func TestHandleMessage_MultiSampleBatch(t *testing.T) {
	resetDB(t)
	handler := makeHandler(testPool)

	payload := batchPayload(t, "station-000", nowISO(), []Observation{
		{Ts: "2026-05-26T12:00:00Z", TempC: 20.0, HumidityPct: 50.0, PressureHPa: 1010.0},
		{Ts: "2026-05-26T12:01:00Z", TempC: 21.0, HumidityPct: 51.0, PressureHPa: 1011.0},
		{Ts: "2026-05-26T12:02:00Z", TempC: 22.0, HumidityPct: 52.0, PressureHPa: 1012.0},
	})
	handler(nil, mockMsg{topic: "weather/station-000/telemetry", payload: payload})

	var count int
	if err := testPool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM readings WHERE station_id = 'station-000'`).Scan(&count); err != nil {
		t.Fatalf("query readings: %v", err)
	}
	if count != 3 {
		t.Errorf("expected 3 readings for 3-sample batch, got %d", count)
	}
}

// latest_station_readings is populated after the first message.
func TestHandleMessage_PopulatesLatest(t *testing.T) {
	resetDB(t)
	handler := makeHandler(testPool)

	payload := batchPayload(t, "station-000", nowISO(), []Observation{
		{Ts: "2026-05-26T12:00:00Z", TempC: 25.0, HumidityPct: 60.0, PressureHPa: 1005.0},
	})
	handler(nil, mockMsg{topic: "weather/station-000/telemetry", payload: payload})

	var count int
	if err := testPool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM latest_station_readings WHERE station_id = 'station-000'`).Scan(&count); err != nil {
		t.Fatalf("query latest: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 row in latest_station_readings, got %d", count)
	}
}

// A newer sample must replace the existing latest row.
func TestHandleMessage_LatestUpdatedByNewerSample(t *testing.T) {
	resetDB(t)
	handler := makeHandler(testPool)

	payload1 := batchPayload(t, "station-000", nowISO(), []Observation{
		{Ts: "2026-05-26T12:00:00Z", TempC: 20.0, HumidityPct: 50.0, PressureHPa: 1000.0},
	})
	handler(nil, mockMsg{topic: "weather/station-000/telemetry", payload: payload1})

	payload2 := batchPayload(t, "station-000", nowISO(), []Observation{
		{Ts: "2026-05-26T12:10:00Z", TempC: 30.0, HumidityPct: 70.0, PressureHPa: 1010.0},
	})
	handler(nil, mockMsg{topic: "weather/station-000/telemetry", payload: payload2})

	var tempF float64
	if err := testPool.QueryRow(context.Background(),
		`SELECT temperature_f FROM latest_station_readings WHERE station_id = 'station-000'`).Scan(&tempF); err != nil {
		t.Fatalf("query latest: %v", err)
	}
	if tempF != cToF(30.0) {
		t.Errorf("expected latest temp to be %.2f°F (newer sample), got %.2f°F", cToF(30.0), tempF)
	}
}

// An older sample arriving after a newer one must not overwrite latest.
func TestHandleMessage_LatestNotOverwrittenByOlderSample(t *testing.T) {
	resetDB(t)
	handler := makeHandler(testPool)

	// Newer arrives first.
	payload1 := batchPayload(t, "station-000", nowISO(), []Observation{
		{Ts: "2026-05-26T12:10:00Z", TempC: 30.0, HumidityPct: 70.0, PressureHPa: 1010.0},
	})
	handler(nil, mockMsg{topic: "weather/station-000/telemetry", payload: payload1})

	// Older arrives second (e.g. replayed or out-of-order).
	payload2 := batchPayload(t, "station-000", nowISO(), []Observation{
		{Ts: "2026-05-26T12:00:00Z", TempC: 20.0, HumidityPct: 50.0, PressureHPa: 1000.0},
	})
	handler(nil, mockMsg{topic: "weather/station-000/telemetry", payload: payload2})

	var tempF float64
	if err := testPool.QueryRow(context.Background(),
		`SELECT temperature_f FROM latest_station_readings WHERE station_id = 'station-000'`).Scan(&tempF); err != nil {
		t.Fatalf("query latest: %v", err)
	}
	if tempF != cToF(30.0) {
		t.Errorf("expected latest temp to remain %.2f°F (newer sample), got %.2f°F", cToF(30.0), tempF)
	}
}

// A batch with no samples must write nothing to the DB.
func TestHandleMessage_EmptySamples_NoWrite(t *testing.T) {
	resetDB(t)
	handler := makeHandler(testPool)

	payload := batchPayload(t, "station-000", nowISO(), []Observation{})
	handler(nil, mockMsg{topic: "weather/station-000/telemetry", payload: payload})

	var count int
	if err := testPool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM readings`).Scan(&count); err != nil {
		t.Fatalf("query readings: %v", err)
	}
	if count != 0 {
		t.Errorf("expected no readings for empty samples batch, got %d", count)
	}
}

// Invalid JSON must not cause any DB writes.
func TestHandleMessage_InvalidJSON_NoWrite(t *testing.T) {
	resetDB(t)
	handler := makeHandler(testPool)

	handler(nil, mockMsg{
		topic:   "weather/station-000/telemetry",
		payload: []byte("this is not json"),
	})

	var count int
	if err := testPool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM readings`).Scan(&count); err != nil {
		t.Fatalf("query readings: %v", err)
	}
	if count != 0 {
		t.Errorf("expected no readings for invalid JSON, got %d", count)
	}
}

// A malformed timestamp on one sample skips that sample; valid siblings are still written.
func TestHandleMessage_BadTimestamp_ValidSamplesStillWritten(t *testing.T) {
	resetDB(t)
	handler := makeHandler(testPool)

	payload := batchPayload(t, "station-000", nowISO(), []Observation{
		{Ts: "2026-05-26T12:00:00Z", TempC: 20.0, HumidityPct: 50.0, PressureHPa: 1010.0},
		{Ts: "not-a-timestamp", TempC: 21.0, HumidityPct: 51.0, PressureHPa: 1011.0},
		{Ts: "2026-05-26T12:02:00Z", TempC: 22.0, HumidityPct: 52.0, PressureHPa: 1012.0},
	})
	handler(nil, mockMsg{topic: "weather/station-000/telemetry", payload: payload})

	var count int
	if err := testPool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM readings WHERE station_id = 'station-000'`).Scan(&count); err != nil {
		t.Fatalf("query readings: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 readings (bad-ts sample skipped), got %d", count)
	}
}
