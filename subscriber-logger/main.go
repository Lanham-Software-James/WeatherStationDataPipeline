package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type Telemetry struct {
	StationID   string  `json:"station_id"`
	Timestamp   string  `json:"timestamp"`
	TempC       float64 `json:"temperature_c"`
	HumidityPct float64 `json:"humidity_pct"`
	PressureHPa float64 `json:"pressure_hpa"`
}

const subscribeTopic = "weather/+/telemetry"

func handler(_ mqtt.Client, msg mqtt.Message) {
	var t Telemetry
	if err := json.Unmarshal(msg.Payload(), &t); err != nil {
		fmt.Printf("[%s] (unparseable) %s\n", msg.Topic(), string(msg.Payload()))
		return
	}
	fmt.Printf(
		"[%s] station=%-12s  temp=%5.1f°C  humidity=%5.1f%%  pressure=%7.1fhPa  wind=%4.1fm/s @ %5.1f°  rain=%4.1fmm  ts=%s\n",
		msg.Topic(),
		t.StationID,
		t.TempC,
		t.HumidityPct,
		t.PressureHPa,
		t.Timestamp,
	)
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
		SetDefaultPublishHandler(handler).
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

	client := connect(broker, "subscriber-logger")
	defer client.Disconnect(250)

	if token := client.Subscribe(subscribeTopic, 1, nil); token.Wait() && token.Error() != nil {
		fmt.Printf("subscribe error: %v\n", token.Error())
		os.Exit(1)
	}

	fmt.Printf("subscriber-logger ready — broker=%s  listening on %s\n", broker, subscribeTopic)
	select {}
}
