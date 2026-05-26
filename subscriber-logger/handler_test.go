package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestProcessPayload_SingleSample(t *testing.T) {
	payload := []byte(`{
		"station_id": "station-000",
		"sent_at":    "2026-05-26T23:01:45Z",
		"samples": [
			{"ts": "2026-05-26T23:01:45Z", "temperature_c": 23.5, "humidity_pct": 53.5, "pressure_hpa": 989.3}
		]
	}`)

	var buf bytes.Buffer
	processPayload("weather/station-000/telemetry", payload, &buf)

	out := buf.String()
	lines := strings.Split(strings.TrimSpace(out), "\n")

	if len(lines) != 1 {
		t.Fatalf("expected 1 log line, got %d: %q", len(lines), out)
	}
	for _, want := range []string{"station-000", "23.5", "53.5", "989.3", "2026-05-26T23:01:45Z"} {
		if !strings.Contains(out, want) {
			t.Errorf("expected %q in output, got: %s", want, out)
		}
	}
}

func TestProcessPayload_MultipleSamples(t *testing.T) {
	payload := []byte(`{
		"station_id": "station-000",
		"sent_at":    "2026-05-26T23:01:45Z",
		"samples": [
			{"ts": "2026-05-26T23:01:43Z", "temperature_c": 21.0, "humidity_pct": 50.0, "pressure_hpa": 988.0},
			{"ts": "2026-05-26T23:01:44Z", "temperature_c": 22.0, "humidity_pct": 51.0, "pressure_hpa": 988.5},
			{"ts": "2026-05-26T23:01:45Z", "temperature_c": 23.0, "humidity_pct": 52.0, "pressure_hpa": 989.0}
		]
	}`)

	var buf bytes.Buffer
	processPayload("weather/station-000/telemetry", payload, &buf)

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 3 {
		t.Errorf("expected 1 log line per sample (3 total), got %d", len(lines))
	}
}

func TestProcessPayload_UnparseablePayload(t *testing.T) {
	var buf bytes.Buffer
	processPayload("weather/station-000/telemetry", []byte("not valid json"), &buf)

	out := buf.String()
	if !strings.Contains(out, "(unparseable)") {
		t.Errorf("expected (unparseable) marker in output, got: %s", out)
	}
}

func TestProcessPayload_EmptySamples(t *testing.T) {
	payload := []byte(`{"station_id": "station-000", "sent_at": "2026-05-26T23:01:45Z", "samples": []}`)

	var buf bytes.Buffer
	processPayload("weather/station-000/telemetry", payload, &buf)

	if buf.Len() != 0 {
		t.Errorf("expected no output for empty samples array, got: %s", buf.String())
	}
}
