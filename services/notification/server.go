package main

import (
	"context"
	"log/slog"
	"strings"

	eventpb "surges-entry/proto/gen/event"

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
	if strings.TrimSpace(req.UserId) == "" {
		return nil, status.Error(codes.InvalidArgument, "user_id cannot be empty")
	}

	// Extract OpenTelemetry trace ID from incoming gRPC context without overwriting
	// the event's correlation/idempotency trace_id.
	var otelTraceID string
	span := trace.SpanFromContext(ctx)
	if span.SpanContext().IsValid() {
		tid := span.SpanContext().TraceID().String()
		if tid != "" && tid != "00000000000000000000000000000000" {
			otelTraceID = tid
		}
	}

	logAttrs := []any{
		"alert", true,
		"alert_type", "anomaly",
		"user_id", req.UserId,
		"event_type", req.EventType,
		"value", req.EventValue,
		"trace_id", req.TraceId,
		"timestamp", req.Timestamp,
	}
	if otelTraceID != "" {
		logAttrs = append(logAttrs, "otel_trace_id", otelTraceID)
	}

	// In production, outbound notification integrations (PagerDuty, Slack, Webhooks) dispatch asynchronously via retry queues.
	s.logger.InfoContext(ctx, "ANOMALY_ALERT", logAttrs...)

	return &eventpb.AlertResponse{
		Success: true,
		Message: "alert processed",
	}, nil
}
