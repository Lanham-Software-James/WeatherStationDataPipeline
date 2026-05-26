package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/jackc/pgx/v5/pgxpool"
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

func cToF(c float64) float64 { return c*9/5 + 32 }

func mustEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func makeHandler(pool *pgxpool.Pool) mqtt.MessageHandler {
	return func(_ mqtt.Client, msg mqtt.Message) {
		var batch ObservationBatch
		if err := json.Unmarshal(msg.Payload(), &batch); err != nil {
			fmt.Printf("[subscriber-db] unparseable payload on %s: %v\n", msg.Topic(), err)
			return
		}
		if len(batch.Samples) == 0 {
			return
		}

		ctx := context.Background()
		tx, err := pool.Begin(ctx)
		if err != nil {
			fmt.Printf("[subscriber-db] begin tx error: %v\n", err)
			return
		}
		defer tx.Rollback(ctx)

		if _, err = tx.Exec(ctx,
			`INSERT INTO stations (id, name) VALUES ($1, $1) ON CONFLICT (id) DO NOTHING`,
			batch.StationID,
		); err != nil {
			fmt.Printf("[subscriber-db] station upsert error: %v\n", err)
			return
		}

		for _, s := range batch.Samples {
			recordedAt, err := time.Parse(time.RFC3339, s.Ts)
			if err != nil {
				fmt.Printf("[subscriber-db] bad ts %q: %v — skipping sample\n", s.Ts, err)
				continue
			}

			sampleJSON, err := json.Marshal(s)
			if err != nil {
				fmt.Printf("[subscriber-db] marshal sample error: %v — skipping sample\n", err)
				continue
			}

			var readingID int64
			err = tx.QueryRow(ctx, `
				INSERT INTO readings
					(station_id, recorded_at, temperature_f, humidity_percent, pressure_hpa, raw_payload)
				VALUES ($1, $2, $3, $4, $5, $6)
				RETURNING id`,
				batch.StationID,
				recordedAt,
				cToF(s.TempC),
				s.HumidityPct,
				s.PressureHPa,
				sampleJSON,
			).Scan(&readingID)
			if err != nil {
				fmt.Printf("[subscriber-db] readings insert error: %v\n", err)
				return
			}

			_, err = tx.Exec(ctx, `
				INSERT INTO latest_station_readings
					(station_id, reading_id, recorded_at, received_at,
					 temperature_f, humidity_percent, pressure_hpa, updated_at)
				VALUES ($1, $2, $3, now(), $4, $5, $6, now())
				ON CONFLICT (station_id) DO UPDATE SET
					reading_id       = EXCLUDED.reading_id,
					recorded_at      = EXCLUDED.recorded_at,
					received_at      = EXCLUDED.received_at,
					temperature_f    = EXCLUDED.temperature_f,
					humidity_percent = EXCLUDED.humidity_percent,
					pressure_hpa     = EXCLUDED.pressure_hpa,
					updated_at       = now()
				WHERE latest_station_readings.recorded_at < EXCLUDED.recorded_at`,
				batch.StationID,
				readingID,
				recordedAt,
				cToF(s.TempC),
				s.HumidityPct,
				s.PressureHPa,
			)
			if err != nil {
				fmt.Printf("[subscriber-db] latest upsert error: %v\n", err)
				return
			}

			fmt.Printf("[subscriber-db] reading_id=%d station=%s recorded_at=%s temp=%.1f°F\n",
				readingID, batch.StationID, recordedAt.Format(time.RFC3339), cToF(s.TempC))
		}

		if err = tx.Commit(ctx); err != nil {
			fmt.Printf("[subscriber-db] commit error: %v\n", err)
			return
		}
	}
}

func connect(broker, clientID string, handler mqtt.MessageHandler) mqtt.Client {
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
	dbURL := mustEnv("DATABASE_URL", "postgres://weather:weather@localhost:5432/weather?sslmode=disable")

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		fmt.Printf("db connect error: %v\n", err)
		os.Exit(1)
	}
	defer pool.Close()

	handler := makeHandler(pool)
	client := connect(broker, "subscriber-db", handler)
	defer client.Disconnect(250)

	if token := client.Subscribe(subscribeTopic, 1, nil); token.Wait() && token.Error() != nil {
		fmt.Printf("subscribe error: %v\n", token.Error())
		os.Exit(1)
	}

	fmt.Printf("subscriber-db ready — broker=%s\n", broker)
	select {}
}
