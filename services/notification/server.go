package main

import (
	"context"
	"log/slog"

	eventpb "event-platform/proto/gen/event"

	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type NotificationServer struct {
	eventpb.UnimplementedNotificationServiceServer
	logger *slog.Logger
}

func NewNotificationServer(logger *slog.Logger) *NotificationServer {
	return &NotificationServer{
		logger: logger,
	}
}

func (s *NotificationServer) SendAlert(ctx context.Context, req *eventpb.AlertRequest) (*eventpb.AlertResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "empty alert request")
	}
	if req.UserId == "" {
		return nil, status.Error(codes.InvalidArgument, "user_id cannot be empty")
	}

	// Extract OpenTelemetry trace ID from the incoming gRPC context (injected by client interceptor)
	span := trace.SpanFromContext(ctx)
	if span.SpanContext().IsValid() {
		otelTraceID := span.SpanContext().TraceID().String()
		if otelTraceID != "" && otelTraceID != "00000000000000000000000000000000" {
			req.TraceId = otelTraceID
		}
	}

	// In production, outbound notification integrations (PagerDuty, Slack, Webhooks) dispatch asynchronously via retry queues.
	s.logger.InfoContext(ctx, "🚨 ANOMALY_ALERT",
		"alert", true,
		"alert_type", "anomaly",
		"user_id", req.UserId,
		"event_type", req.EventType,
		"value", req.EventValue,
		"trace_id", req.TraceId,
		"timestamp", req.Timestamp,
	)

	return &eventpb.AlertResponse{
		Success: true,
		Message: "alert processed",
	}, nil
}
