package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	eventpb "event-platform/proto/gen/event"

	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func TestGetEnv(t *testing.T) {
	t.Setenv("TEST_KEY_NOTIF", "custom_val")
	if val := getEnv("TEST_KEY_NOTIF", "default"); val != "custom_val" {
		t.Errorf("expected custom_val, got %s", val)
	}
	if val := getEnv("TEST_NON_EXISTENT", "default"); val != "default" {
		t.Errorf("expected default, got %s", val)
	}
}

func TestNewNotificationHealthServer_Serving(t *testing.T) {
	healthServer := health.NewServer()
	healthServer.SetServingStatus(eventpb.NotificationService_ServiceDesc.ServiceName, grpc_health_v1.HealthCheckResponse_SERVING)

	srv := newNotificationHealthServer("8080", healthServer)
	if srv == nil {
		t.Fatal("expected non-nil server")
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

func TestNewNotificationHealthServer_NotServing(t *testing.T) {
	healthServer := health.NewServer()
	healthServer.SetServingStatus(eventpb.NotificationService_ServiceDesc.ServiceName, grpc_health_v1.HealthCheckResponse_NOT_SERVING)

	srv := newNotificationHealthServer("8080", healthServer)

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

func TestSetupNotificationGRPCServer(t *testing.T) {
	healthServer := health.NewServer()
	server := NewNotificationServer(slog.New(slog.NewTextHandler(io.Discard, nil)))

	grpcServer, listener, err := setupNotificationGRPCServer("0", server, healthServer)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if grpcServer == nil || listener == nil {
		t.Fatal("expected non-nil grpcServer and listener")
	}
	grpcServer.Stop()
	_ = listener.Close()
}

func TestRunNotificationServer_GracefulShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	errChan := make(chan error, 1)
	go func() {
		// Use port 0 to avoid conflicts
		errChan <- run(ctx, "0", "0", logger)
	}()

	// Allow server to spin up
	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-errChan:
		if err != nil {
			t.Fatalf("expected nil error on clean shutdown, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for notification server to shut down")
	}
}
