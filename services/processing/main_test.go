package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	eventpb "event-platform/proto/gen/event"

	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func TestNewProcessingHealthServer_Serving(t *testing.T) {
	healthServer := health.NewServer()
	healthServer.SetServingStatus(eventpb.ProcessingService_ServiceDesc.ServiceName, grpc_health_v1.HealthCheckResponse_SERVING)

	srv := newProcessingHealthServer("8080", healthServer)
	if srv == nil {
		t.Fatal("expected non-nil server")
	}
	if srv.Addr != ":8080" {
		t.Errorf("expected addr :8080, got %s", srv.Addr)
	}

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}
	if rec.Body.String() != "ok" {
		t.Errorf("expected body ok, got %s", rec.Body.String())
	}
}

func TestNewProcessingHealthServer_NotServing(t *testing.T) {
	healthServer := health.NewServer()
	healthServer.SetServingStatus(eventpb.ProcessingService_ServiceDesc.ServiceName, grpc_health_v1.HealthCheckResponse_NOT_SERVING)

	srv := newProcessingHealthServer("8080", healthServer)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("expected status 503, got %d", rec.Code)
	}
	if rec.Body.String() != "unhealthy" {
		t.Errorf("expected body unhealthy, got %s", rec.Body.String())
	}
}

func TestInitNotificationClient(t *testing.T) {
	conn, client, err := initNotificationClient("localhost:50052")
	if err != nil {
		t.Fatalf("unexpected error initializing notification client: %v", err)
	}
	if conn == nil {
		t.Fatal("expected non-nil conn")
	}
	if client == nil {
		t.Fatal("expected non-nil client")
	}
	_ = conn.Close()
}

func TestSetupProcessingGRPCServer(t *testing.T) {
	healthServer := health.NewServer()
	processor := NewEventProcessor(&mockHotStateStore{}, &mockEventRepository{}, &mockNotificationClient{}, Config{}, nil)
	defer processor.Stop()

	server, listener, err := setupProcessingGRPCServer("0", processor, healthServer)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if server == nil {
		t.Fatal("expected non-nil server")
	}
	if listener == nil {
		t.Fatal("expected non-nil listener")
	}
	server.Stop()
	_ = listener.Close()
}

func TestRun_InvalidPostgresDSN(t *testing.T) {
	cfg := Config{
		PostgresDSN: "invalid:::dsn",
	}
	err := run(context.Background(), cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err == nil {
		t.Error("expected error from run with invalid postgres dsn, got nil")
	}
}
