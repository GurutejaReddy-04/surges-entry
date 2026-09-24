package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	eventpb "surges-entry/proto/gen/event"

	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

func TestSendAlert_StructuredLogging(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	server := NewNotificationServer(logger)

	req := &eventpb.AlertRequest{
		UserId:     "user-spike",
		EventType:  "purchase",
		EventValue: 999.99,
		IsAnomaly:  true,
		TraceId:    "test-alert-trace-789",
		Timestamp:  time.Now().Unix(),
	}

	resp, err := server.SendAlert(context.Background(), req)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if !resp.Success {
		t.Errorf("expected success=true, got %v", resp.Success)
	}

	// Verify logged JSON fields
	var logRecord map[string]any
	if err := json.Unmarshal(buf.Bytes(), &logRecord); err != nil {
		t.Fatalf("failed to parse log output as json: %v, raw: %s", err, buf.String())
	}

	if logRecord["alert"] != true {
		t.Errorf("expected alert=true in log, got %v", logRecord["alert"])
	}
	if logRecord["alert_type"] != "anomaly" {
		t.Errorf("expected alert_type=anomaly, got %v", logRecord["alert_type"])
	}
	if logRecord["user_id"] != "user-spike" {
		t.Errorf("expected user_id=user-spike, got %v", logRecord["user_id"])
	}
	if logRecord["trace_id"] != "test-alert-trace-789" {
		t.Errorf("expected trace_id=test-alert-trace-789, got %v", logRecord["trace_id"])
	}
	if logRecord["value"] != 999.99 {
		t.Errorf("expected value=999.99, got %v", logRecord["value"])
	}
}

func TestSendAlert_ValidationErrors(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	server := NewNotificationServer(logger)

	t.Run("nil request", func(t *testing.T) {
		_, err := server.SendAlert(context.Background(), nil)
		if err == nil {
			t.Fatal("expected error on nil request, got nil")
		}
		st, ok := status.FromError(err)
		if !ok || st.Code() != codes.InvalidArgument {
			t.Errorf("expected InvalidArgument code, got %v", err)
		}
	})

	t.Run("empty user_id", func(t *testing.T) {
		_, err := server.SendAlert(context.Background(), &eventpb.AlertRequest{
			UserId: "",
		})
		if err == nil {
			t.Fatal("expected error on empty user_id, got nil")
		}
		st, ok := status.FromError(err)
		if !ok || st.Code() != codes.InvalidArgument {
			t.Errorf("expected InvalidArgument code, got %v", err)
		}
	})
}

func TestSendAlert_OTelSpanExtraction(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	server := NewNotificationServer(logger)

	traceIDHex := "4bf92f3577b34da6a3ce929d0e0e4736"
	traceID, err := trace.TraceIDFromHex(traceIDHex)
	if err != nil {
		t.Fatalf("failed to create trace ID: %v", err)
	}

	spanCtx := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     trace.SpanID{1, 2, 3, 4, 5, 6, 7, 8},
		TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), spanCtx)

	req := &eventpb.AlertRequest{
		UserId:     "user-otel",
		EventType:  "test",
		EventValue: 123.45,
		TraceId:    "original-trace",
	}

	resp, err := server.SendAlert(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.Success {
		t.Error("expected success")
	}
	if req.TraceId != traceIDHex {
		t.Errorf("expected trace id %s, got %s", traceIDHex, req.TraceId)
	}
}

func TestHealthCheck(t *testing.T) {
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	healthServer.SetServingStatus(eventpb.NotificationService_ServiceDesc.ServiceName, grpc_health_v1.HealthCheckResponse_SERVING)

	resp, err := healthServer.Check(context.Background(), &grpc_health_v1.HealthCheckRequest{
		Service: eventpb.NotificationService_ServiceDesc.ServiceName,
	})
	if err != nil {
		t.Fatalf("health check failed: %v", err)
	}

	if resp.Status != grpc_health_v1.HealthCheckResponse_SERVING {
		t.Errorf("expected status SERVING, got %v", resp.Status)
	}
}
