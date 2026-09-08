package main

import (
	"os"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	// Default config
	cfg := LoadConfig()
	if cfg.GRPCPort != "50051" {
		t.Errorf("expected default 50051, got %s", cfg.GRPCPort)
	}
	if cfg.RedisAddr != "localhost:6379" {
		t.Errorf("expected localhost:6379, got %s", cfg.RedisAddr)
	}
	if cfg.FallbackThreshold != 1000.0 {
		t.Errorf("expected 1000.0, got %f", cfg.FallbackThreshold)
	}
	if cfg.DeviationMultiplier != 1.5 {
		t.Errorf("expected 1.5, got %f", cfg.DeviationMultiplier)
	}
	if cfg.RedisWindowSize != 10 {
		t.Errorf("expected 10, got %d", cfg.RedisWindowSize)
	}
	if cfg.NotificationAddr != "localhost:50052" {
		t.Errorf("expected localhost:50052, got %s", cfg.NotificationAddr)
	}

	// Custom environment variables
	os.Setenv("GRPC_PORT", "60051")
	os.Setenv("REDIS_ADDR", "myredis:6379")
	os.Setenv("POSTGRES_DSN", "postgres://custom")
	os.Setenv("FALLBACK_THRESHOLD", "500.5")
	os.Setenv("DEVIATION_MULTIPLIER", "2.0")
	os.Setenv("REDIS_WINDOW_SIZE", "20")
	os.Setenv("NOTIFICATION_GRPC_ADDR", "notif:50052")
	defer func() {
		os.Unsetenv("GRPC_PORT")
		os.Unsetenv("REDIS_ADDR")
		os.Unsetenv("POSTGRES_DSN")
		os.Unsetenv("FALLBACK_THRESHOLD")
		os.Unsetenv("DEVIATION_MULTIPLIER")
		os.Unsetenv("REDIS_WINDOW_SIZE")
		os.Unsetenv("NOTIFICATION_GRPC_ADDR")
	}()

	customCfg := LoadConfig()
	if customCfg.GRPCPort != "60051" || customCfg.RedisAddr != "myredis:6379" || customCfg.FallbackThreshold != 500.5 || customCfg.DeviationMultiplier != 2.0 || customCfg.RedisWindowSize != 20 || customCfg.NotificationAddr != "notif:50052" {
		t.Errorf("custom config mismatch: %+v", customCfg)
	}

	// Fallback on invalid numbers
	os.Setenv("FALLBACK_THRESHOLD", "invalid-float")
	os.Setenv("REDIS_WINDOW_SIZE", "invalid-int")
	fallbackCfg := LoadConfig()
	if fallbackCfg.FallbackThreshold != 1000.0 {
		t.Errorf("expected fallback float 1000.0, got %f", fallbackCfg.FallbackThreshold)
	}
	if fallbackCfg.RedisWindowSize != 10 {
		t.Errorf("expected fallback int 10, got %d", fallbackCfg.RedisWindowSize)
	}
}
