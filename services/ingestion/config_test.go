package main

import (
	"os"
	"testing"
	"time"
)

func TestLoadConfig(t *testing.T) {
	// Test defaults
	cfg := LoadConfig()
	if cfg.KafkaTopic != "events" {
		t.Errorf("expected default topic events, got %s", cfg.KafkaTopic)
	}
	if cfg.ConsumerGroupID != "ingestion-service-group" {
		t.Errorf("expected default group ingestion-service-group, got %s", cfg.ConsumerGroupID)
	}
	if cfg.ProcessingTarget != "localhost:50051" {
		t.Errorf("expected default target localhost:50051, got %s", cfg.ProcessingTarget)
	}
	if cfg.GRPCTimeout != 5*time.Second {
		t.Errorf("expected 5s timeout, got %v", cfg.GRPCTimeout)
	}
	if len(cfg.KafkaBrokers) == 0 {
		t.Errorf("expected at least 1 broker")
	}

	// Test custom env vars
	os.Setenv("KAFKA_BROKERS", "broker1:9092, broker2:9092")
	os.Setenv("KAFKA_TOPIC", "my-topic")
	os.Setenv("KAFKA_CONSUMER_GROUP", "my-group")
	os.Setenv("PROCESSING_ADDR", "processing:50051")
	defer func() {
		os.Unsetenv("KAFKA_BROKERS")
		os.Unsetenv("KAFKA_TOPIC")
		os.Unsetenv("KAFKA_CONSUMER_GROUP")
		os.Unsetenv("PROCESSING_ADDR")
	}()

	customCfg := LoadConfig()
	if customCfg.KafkaTopic != "my-topic" {
		t.Errorf("expected my-topic, got %s", customCfg.KafkaTopic)
	}
	if customCfg.ConsumerGroupID != "my-group" {
		t.Errorf("expected my-group, got %s", customCfg.ConsumerGroupID)
	}
	if customCfg.ProcessingTarget != "processing:50051" {
		t.Errorf("expected processing:50051, got %s", customCfg.ProcessingTarget)
	}
	if len(customCfg.KafkaBrokers) != 2 || customCfg.KafkaBrokers[0] != "broker1:9092" || customCfg.KafkaBrokers[1] != "broker2:9092" {
		t.Errorf("unexpected brokers: %v", customCfg.KafkaBrokers)
	}
}

func TestNewSaramaConfig(t *testing.T) {
	cfg := newSaramaConfig()
	if cfg == nil {
		t.Fatal("expected non-nil sarama config")
	}
	if !cfg.Consumer.Return.Errors {
		t.Errorf("expected Return.Errors=true")
	}
}
