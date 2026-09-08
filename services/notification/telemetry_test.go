package main

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
)

func TestInitTelemetry(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))

	os.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4317")
	os.Setenv("OTEL_SERVICE_NAME", "test-notification")
	defer func() {
		os.Unsetenv("OTEL_EXPORTER_OTLP_ENDPOINT")
		os.Unsetenv("OTEL_SERVICE_NAME")
	}()

	shutdown, err := InitTelemetry(context.Background(), "default-notification", logger)
	if err != nil {
		t.Fatalf("unexpected error initializing telemetry: %v", err)
	}
	if shutdown == nil {
		t.Fatal("expected non-nil shutdown function")
	}
	defer shutdown(context.Background())
}
