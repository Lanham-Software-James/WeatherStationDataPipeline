package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type Telemetry struct {
	StationID   string  `json:"station_id"`
	Timestamp   string  `json:"timestamp"`
	TempC       float64 `json:"temperature_c"`
	HumidityPct float64 `json:"humidity_pct"`
	PressureHPa float64 `json:"pressure_hpa"`
	WindSpeedMS float64 `json:"wind_speed_ms"`
	WindDirDeg  float64 `json:"wind_dir_deg"`
	RainfallMM  float64 `json:"rainfall_mm"`
}

func round2(v float64) float64 {
	return float64(int(v*100)) / 100
}

func fakePayload(stationID string) Telemetry {
	return Telemetry{
		StationID:   stationID,
		Timestamp:   time.Now().UTC().Format(time.RFC3339),
		TempC:       round2(15.0 + rand.Float64()*20.0),
		HumidityPct: round2(40.0 + rand.Float64()*50.0),
		PressureHPa: round2(1000.0 + rand.Float64()*40.0),
		WindSpeedMS: round2(rand.Float64() * 15.0),
		WindDirDeg:  round2(rand.Float64() * 360.0),
		RainfallMM:  round2(rand.Float64() * 5.0),
	}
}

func mustEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func connect(broker, clientID string) mqtt.Client {
	opts := mqtt.NewClientOptions().
		AddBroker(broker).
		SetClientID(clientID).
		SetConnectRetry(true).
		SetConnectRetryInterval(2 * time.Second)

	client := mqtt.NewClient(opts)
	for {
		if token := client.Connect(); token.Wait() && token.Error() != nil {
			fmt.Printf("connect failed: %v — retrying\n", token.Error())
			time.Sleep(2 * time.Second)
			continue
		}
		return client
	}
}

func main() {
	broker := mustEnv("MQTT_BROKER", "tcp://localhost:1883")
	stationID := mustEnv("STATION_ID", "station-001")
	intervalSec, _ := strconv.Atoi(mustEnv("PUBLISH_INTERVAL_SEC", "5"))
	if intervalSec <= 0 {
		intervalSec = 5
	}

	client := connect(broker, "publisher-sim-"+stationID)
	defer client.Disconnect(250)

	topic := fmt.Sprintf("weather/%s/telemetry", stationID)
	fmt.Printf("publisher-sim ready — broker=%s topic=%s interval=%ds\n", broker, topic, intervalSec)

	ticker := time.NewTicker(time.Duration(intervalSec) * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		payload := fakePayload(stationID)
		data, err := json.Marshal(payload)
		if err != nil {
			fmt.Printf("marshal error: %v\n", err)
			continue
		}
		if token := client.Publish(topic, 1, false, data); token.Wait() && token.Error() != nil {
			fmt.Printf("publish error: %v\n", token.Error())
			continue
		}
		fmt.Printf("→ %s  %s\n", topic, string(data))
	}
}
