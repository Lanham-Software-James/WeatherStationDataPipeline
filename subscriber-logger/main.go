package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

const subscribeTopic = "weather/+/telemetry"

type Observation struct {
	Ts          string  `json:"ts"`
	TempC       float64 `json:"temperature_c"`
	HumidityPct float64 `json:"humidity_pct"`
	PressureHPa float64 `json:"pressure_hpa"`
}

type ObservationBatch struct {
	StationID string        `json:"station_id"`
	SentAt    string        `json:"sent_at"`
	Samples   []Observation `json:"samples"`
}

func processPayload(topic string, payload []byte, w io.Writer) {
	var batch ObservationBatch
	if err := json.Unmarshal(payload, &batch); err != nil {
		fmt.Fprintf(w, "[%s] (unparseable) %s\n", topic, string(payload))
		return
	}

	for _, s := range batch.Samples {
		fmt.Fprintf(w,
			"[%s] station=%-12s  temp=%5.1f°C  humidity=%5.1f%%  pressure=%7.1fhPa  ts=%s  sent_at=%s\n",
			topic,
			batch.StationID,
			s.TempC,
			s.HumidityPct,
			s.PressureHPa,
			s.Ts,
			batch.SentAt,
		)
	}
}

func handler(_ mqtt.Client, msg mqtt.Message) {
	processPayload(msg.Topic(), msg.Payload(), os.Stdout)
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
