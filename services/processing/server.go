package main

import (
	"context"
	"log/slog"
	"math"
	"sync"
	"time"

	eventpb "surges-entry/proto/gen/event"

	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type NotificationClient interface {
	SendAlert(ctx context.Context, in *eventpb.AlertRequest, opts ...grpc.CallOption) (*eventpb.AlertResponse, error)
}

type alertTask struct {
	req     *eventpb.AlertRequest
	spanCtx trace.SpanContext
}

type EventProcessor struct {
	eventpb.UnimplementedProcessingServiceServer
	hotState           HotStateStore
	repo               EventRepository
	notificationClient NotificationClient
	config             Config
	logger             *slog.Logger
	alertQueue         chan *alertTask
	stopWorkers        chan struct{}
	workerWg           sync.WaitGroup
}

func NewEventProcessor(
	hotState HotStateStore,
	repo EventRepository,
	notificationClient NotificationClient,
	config Config,
	logger *slog.Logger,
) *EventProcessor {
	p := &EventProcessor{
		hotState:           hotState,
		repo:               repo,
		notificationClient: notificationClient,
		config:             config,
		logger:             logger,
		alertQueue:         make(chan *alertTask, 1000),
		stopWorkers:        make(chan struct{}),
	}

	if notificationClient != nil {
		p.startAlertWorkers(4)
	}

	return p
}

func (s *EventProcessor) startAlertWorkers(numWorkers int) {
	for i := 0; i < numWorkers; i++ {
		s.workerWg.Add(1)
		go func() {
			defer s.workerWg.Done()
			for {
				select {
				case <-s.stopWorkers:
					return
				case task, ok := <-s.alertQueue:
					if !ok {
						return
					}
					s.dispatchAlert(task)
				}
			}
		}()
	}
}

func (s *EventProcessor) dispatchAlert(task *alertTask) {
	workerCtx := context.Background()
	if task.spanCtx.IsValid() {
		// Use ContextWithSpanContext to propagate in-process OpenTelemetry span metadata to worker
		workerCtx = trace.ContextWithSpanContext(workerCtx, task.spanCtx)
	}
	ctx, cancel := context.WithTimeout(workerCtx, 3*time.Second)
	defer cancel()

	_, err := s.notificationClient.SendAlert(ctx, task.req)
	if err != nil {
		s.logger.WarnContext(ctx, "failed to dispatch alert to notification service",
			"user_id", task.req.UserId,
			"trace_id", task.req.TraceId,
			"error", err,
		)
	} else {
		s.logger.InfoContext(ctx, "anomaly alert dispatched to notification service",
			"user_id", task.req.UserId,
			"trace_id", task.req.TraceId,
		)
	}
}

func (s *EventProcessor) Stop() {
	close(s.stopWorkers)
	s.workerWg.Wait()
}

func (s *EventProcessor) ProcessEvent(ctx context.Context, req *eventpb.Event) (*eventpb.ProcessResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "empty request payload")
	}

	if req.UserId == "" {
		return nil, status.Error(codes.InvalidArgument, "missing user_id in event")
	}

	// Poison pill protection: Prevent out-of-range floats from crashing PostgreSQL DECIMAL(10,2)
	if math.IsNaN(req.Value) || math.IsInf(req.Value, 0) || math.Abs(req.Value) > 99999999.99 {
		return nil, status.Error(codes.InvalidArgument, "value out of supported numeric range [-99999999.99, 99999999.99]")
	}

	// Extract OpenTelemetry trace ID from the incoming gRPC context (injected by client interceptor)
	span := trace.SpanFromContext(ctx)
	if span.SpanContext().IsValid() {
		otelTraceID := span.SpanContext().TraceID().String()
		if otelTraceID != "" && otelTraceID != "00000000000000000000000000000000" {
			req.TraceId = otelTraceID
		}
	}

	// 1. Hot-state cache: Read rolling average BEFORE persistence.
	// Never mutates Redis state prior to DB confirmation, preventing dual-write duplication on retries.
	// Returns nil if Redis is unavailable or on user cold start.
	rollingAvg := s.hotState.ComputeAverage(ctx, req.UserId, req.TraceId)

	// 2. Anomaly detection: Two-tier evaluation against pre-existing window
	isAnomaly := DetermineAnomaly(req.Value, rollingAvg, s.config.DeviationMultiplier, s.config.FallbackThreshold)

	// 3. PostgreSQL persistence: Durable audit record with retry
	record := &ProcessedEventRecord{
		UserID:     req.UserId,
		EventType:  req.EventType,
		EventValue: req.Value,
		RollingAvg: rollingAvg,
		IsAnomaly:  isAnomaly,
		TraceID:    req.TraceId,
	}

	var dbErr error
	backoff := 50 * time.Millisecond
	maxAttempts := 3

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		dbErr = s.repo.SaveProcessedEvent(ctx, record)
		if dbErr == nil {
			break
		}

		s.logger.WarnContext(ctx, "failed to persist processed event to database, retrying",
			"attempt", attempt,
			"max_attempts", maxAttempts,
			"user_id", req.UserId,
			"trace_id", req.TraceId,
			"error", dbErr,
		)

		if attempt < maxAttempts {
			select {
			case <-ctx.Done():
				return nil, status.Errorf(codes.Canceled, "context canceled during db retry: %v", ctx.Err())
			case <-time.After(backoff):
				backoff *= 2
			}
		}
	}

	if dbErr != nil {
		s.logger.ErrorContext(ctx, "failed to persist processed event after retries; returning gRPC error to prevent offset commit",
			"user_id", req.UserId,
			"trace_id", req.TraceId,
			"error", dbErr,
		)
		return nil, status.Errorf(codes.Unavailable, "database persistence failed: %v", dbErr)
	}

	// 4. Redis Hot State Commit: Executed ONLY after successful database write.
	// Normal values are committed to the window. Anomalies only refresh TTL to avoid polluting future baselines.
	if commitErr := s.hotState.CommitEvent(ctx, req.UserId, req.Value, isAnomaly, req.TraceId); commitErr != nil {
		s.logger.WarnContext(ctx, "failed to commit event to redis hot state window (audit record safely in postgres)",
			"user_id", req.UserId,
			"trace_id", req.TraceId,
			"error", commitErr,
		)
	}

	// 5. Notification trigger: Asynchronously queue alert if isAnomaly == true
	if isAnomaly && s.notificationClient != nil {
		alertReq := &eventpb.AlertRequest{
			UserId:     req.UserId,
			EventType:  req.EventType,
			EventValue: req.Value,
			IsAnomaly:  true,
			TraceId:    req.TraceId,
			Timestamp:  req.Timestamp,
		}

		task := &alertTask{
			req:     alertReq,
			spanCtx: span.SpanContext(),
		}

		select {
		case s.alertQueue <- task:
			// Queued asynchronously with preserved OpenTelemetry span context
		default:
			s.logger.WarnContext(ctx, "alert queue full, dropping notification dispatch",
				"user_id", req.UserId,
				"trace_id", req.TraceId,
			)
		}
	}

	s.logger.InfoContext(ctx, "event processed",
		"trace_id", req.TraceId,
		"user_id", req.UserId,
		"event_type", req.EventType,
		"value", req.Value,
		"rolling_avg", rollingAvg,
		"is_anomaly", isAnomaly,
	)

	return &eventpb.ProcessResponse{
		Success:     true,
		Message:     "event processed successfully",
		ProcessedAt: time.Now().Unix(),
		IsAnomaly:   isAnomaly,
	}, nil
}
