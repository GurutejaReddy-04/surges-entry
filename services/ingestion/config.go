package main

import (
	"os"
	"strings"
	"time"
)

type Config struct {
	KafkaBrokers     []string
	KafkaTopic       string
	ConsumerGroupID  string
	ProcessingTarget string
	GRPCTimeout      time.Duration
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

func LoadConfig() Config {
	brokerStr := getEnv("KAFKA_BROKERS", getEnv("KAFKA_BROKER", "localhost:9092"))
	brokers := strings.Split(brokerStr, ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
	}

	return Config{
		KafkaBrokers:     brokers,
		KafkaTopic:       getEnv("KAFKA_TOPIC", "events"),
		ConsumerGroupID:  getEnv("KAFKA_CONSUMER_GROUP", "ingestion-service-group"),
		ProcessingTarget: getEnv("PROCESSING_ADDR", "localhost:50051"),
		GRPCTimeout:      5 * time.Second,
	}
}
