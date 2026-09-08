package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

const topic = "events"

type Event struct {
	UserID    string  `json:"user_id"`
	EventType string  `json:"event_type"`
	Value     float64 `json:"value"`
	Timestamp string  `json:"timestamp"`
}

type Config struct {
	KafkaBroker string
	EventRate   int
	NumUsers    int
}

func loadConfig() Config {
	return Config{
		KafkaBroker: envOr("KAFKA_BROKER", "localhost:9092"),
		EventRate:   envIntOr("EVENT_RATE", 5),
		NumUsers:    envIntOr("NUM_USERS", 10),
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envIntOr(key string, fallback int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return fallback
	}
	return n
}

func buildUserPool(n int) []string {
	pool := make([]string, n)
	for i := range pool {
		pool[i] = fmt.Sprintf("user-%03d", i+1)
	}
	return pool
}

var eventTypes = []string{"transaction", "login", "page_view", "purchase", "api_call"}

func generateEvent(pool []string) Event {
	// Normal distribution: mean=100, stddev=20.
	// ~5% of events are deliberate outliers to exercise anomaly detection in Phase 3.
	// In production, anomalies wouldn't announce themselves this politely.
	value := 100.0 + rand.NormFloat64()*20.0
	if rand.Float64() < 0.05 {
		value = 200.0 + rand.Float64()*300.0
	}

	return Event{
		UserID:    pool[rand.IntN(len(pool))],
		EventType: eventTypes[rand.IntN(len(eventTypes))],
		Value:     math.Round(value*100) / 100,
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
	}
}

func newProducer(broker string) (sarama.SyncProducer, error) {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Retry.Max = 3
	cfg.Producer.Partitioner = sarama.NewHashPartitioner
	return sarama.NewSyncProducer([]string{broker}, cfg)
}

func run(ctx context.Context, cfg Config, producer sarama.SyncProducer, log *slog.Logger) error {
	pool := buildUserPool(cfg.NumUsers)
	tick := time.NewTicker(time.Second / time.Duration(cfg.EventRate))
	defer tick.Stop()

	log.InfoContext(ctx, "simulator started",
		"broker", cfg.KafkaBroker,
		"rate", cfg.EventRate,
		"users", cfg.NumUsers,
		"topic", topic,
	)

	var published int64
	for {
		select {
		case <-ctx.Done():
			log.InfoContext(ctx, "shutting down", "total_published", published)
			return nil
		case <-tick.C:
			ev := generateEvent(pool)

			data, err := json.Marshal(ev)
			if err != nil {
				log.ErrorContext(ctx, "marshal failed", "error", err)
				continue
			}

			msg := &sarama.ProducerMessage{
				Topic: topic,
				// Keying by userID: Kafka hashes this to a partition, guaranteeing all events
				// for a given user land on the same partition in strict order. The Processing
				// Service's count-based rolling window (Phase 3) depends on this — without it,
				// per-user windows mix events across partitions and the rolling average becomes
				// non-deterministic.
				Key:   sarama.StringEncoder(ev.UserID),
				Value: sarama.ByteEncoder(data),
			}

			partition, offset, err := producer.SendMessage(msg)
			if err != nil {
				log.ErrorContext(ctx, "publish failed",
					"user_id", ev.UserID,
					"error", err,
				)
				continue
			}

			published++
			log.InfoContext(ctx, "published",
				"user_id", ev.UserID,
				"type", ev.EventType,
				"value", ev.Value,
				"partition", partition,
				"offset", offset,
			)
		}
	}
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	cfg := loadConfig()

	producer, err := newProducer(cfg.KafkaBroker)
	if err != nil {
		log.Error("kafka producer init failed", "broker", cfg.KafkaBroker, "error", err)
		os.Exit(1)
	}
	defer producer.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg, producer, log); err != nil {
		log.Error("simulator failed", "error", err)
		os.Exit(1)
	}
}
