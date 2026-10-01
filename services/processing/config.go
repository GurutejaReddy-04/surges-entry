package main

import (
	"os"
	"strconv"
)

type Config struct {
	GRPCPort            string
	RedisAddr           string
	PostgresDSN         string
	FallbackThreshold   float64
	DeviationMultiplier float64
	RedisWindowSize     int
	NotificationAddr    string
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

func getEnvFloat(key string, fallback float64) float64 {
	val := os.Getenv(key)
	if val == "" {
		return fallback
	}
	f, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return fallback
	}
	return f
}

func getEnvInt(key string, fallback int) int {
	val := os.Getenv(key)
	if val == "" {
		return fallback
	}
	i, err := strconv.Atoi(val)
	if err != nil {
		return fallback
	}
	return i
}

func LoadConfig() Config {
	return Config{
		GRPCPort:            getEnv("GRPC_PORT", "50051"),
		RedisAddr:           getEnv("REDIS_ADDR", "localhost:6379"),
		PostgresDSN:         getEnv("POSTGRES_DSN", ""),
		FallbackThreshold:   getEnvFloat("FALLBACK_THRESHOLD", 1000.0),
		DeviationMultiplier: getEnvFloat("DEVIATION_MULTIPLIER", 1.5),
		RedisWindowSize:     getEnvInt("REDIS_WINDOW_SIZE", 10),
		NotificationAddr:    getEnv("NOTIFICATION_GRPC_ADDR", "localhost:50052"),
	}
}
