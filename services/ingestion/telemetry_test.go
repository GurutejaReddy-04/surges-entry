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

	// In test mode, point to invalid/local endpoint or let it create TracerProvider
	os.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4317")
	os.Setenv("OTEL_SERVICE_NAME", "test-ingestion")
	defer func() {
		os.Unsetenv("OTEL_EXPORTER_OTLP_ENDPOINT")
		os.Unsetenv("OTEL_SERVICE_NAME")
	}()

	shutdown, err := InitTelemetry(context.Background(), "default-service", logger)
	if err != nil {
		t.Fatalf("unexpected error initializing telemetry: %v", err)
	}
	if shutdown == nil {
		t.Fatal("expected non-nil shutdown function")
	}
	defer shutdown(context.Background())
}
