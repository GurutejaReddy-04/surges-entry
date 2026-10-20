package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"math"
	"strings"
	"sync"
	"time"

	eventpb "surges-entry/proto/gen/event"

	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	userLockStripes     = 256
	defaultDrainTimeout = 5 * time.Second
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
	stopOnce           sync.Once
	workerWg           sync.WaitGroup
	workerCtx          context.Context
	workerCancel       context.CancelFunc
	drainTimeout       time.Duration
	userLocks          [userLockStripes]chan struct{}
}

func generateFallbackTraceID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%032x", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

func NewEventProcessor(
	hotState HotStateStore,
	repo EventRepository,
	notificationClient NotificationClient,
	config Config,
	logger *slog.Logger,
) *EventProcessor {
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}

	workerCtx, workerCancel := context.WithCancel(context.Background())

	p := &EventProcessor{
		hotState:           hotState,
		repo:               repo,
		notificationClient: notificationClient,
		config:             config,
		logger:             logger,
		alertQueue:         make(chan *alertTask, 1000),
		stopWorkers:        make(chan struct{}),
		workerCtx:          workerCtx,
		workerCancel:       workerCancel,
		drainTimeout:       defaultDrainTimeout,
	}

	for i := range p.userLocks {
		p.userLocks[i] = make(chan struct{}, 1)
	}

	if notificationClient != nil {
		p.startAlertWorkers(4)
	}

	return p
}

// acquireUserLock serializes Read->Evaluate->Persist->Commit per user stripe
// with zero unbounded map growth and full context cancellation support.
func (s *EventProcessor) acquireUserLock(ctx context.Context, userID string) (func(), error) {
	h := fnv.New32a()
	_, _ = h.Write([]byte(userID))
	stripe := s.userLocks[h.Sum32()&(userLockStripes-1)]
	if stripe == nil {
		return func() {}, nil
	}

	select {
	case stripe <- struct{}{}:
		return func() { <-stripe }, nil
	case <-ctx.Done():
		return nil, status.Errorf(codes.Canceled, "context canceled waiting for user state lock: %v", ctx.Err())
	}
}

func (s *EventProcessor) startAlertWorkers(numWorkers int) {
	for i := 0; i < numWorkers; i++ {
		s.workerWg.Add(1)
		go func() {
			defer s.workerWg.Done()
			for {
				// Prioritize shutdown signal so workers deterministically enter drain mode
				select {
				case <-s.stopWorkers:
					s.drainAlertQueue()
					return
				default:
				}

				select {
				case <-s.stopWorkers:
					s.drainAlertQueue()
					return
				case task := <-s.alertQueue:
					s.dispatchAlertWithParent(s.workerCtx, task)
				}
			}
		}()
	}
}

func (s *EventProcessor) drainAlertQueue() {
	for {
		if s.workerCtx != nil && s.workerCtx.Err() != nil {
			return
		}
		select {
		case task := <-s.alertQueue:
			s.dispatchAlertWithParent(s.workerCtx, task)
		default:
			return
		}
	}
}

func (s *EventProcessor) dispatchAlertWithParent(parentCtx context.Context, task *alertTask) {
	if parentCtx == nil {
		parentCtx = context.Background()
	}
	if parentCtx.Err() != nil {
		return
	}

	workerCtx := parentCtx
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
	s.stopOnce.Do(func() {
		timeout := s.drainTimeout
		if timeout <= 0 {
			timeout = defaultDrainTimeout
		}

		close(s.stopWorkers)

		done := make(chan struct{})
		go func() {
			s.workerWg.Wait()
			close(done)
		}()

		timer := time.NewTimer(timeout)
		defer timer.Stop()

		select {
		case <-done:
			if s.workerCancel != nil {
				s.workerCancel()
			}
		case <-timer.C:
			// Drain deadline exceeded: cancel all in-flight and remaining dispatches cleanly
			if s.workerCancel != nil {
				s.workerCancel()
			}
			<-done
		}
	})
}

func (s *EventProcessor) ProcessEvent(ctx context.Context, req *eventpb.Event) (*eventpb.ProcessResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "empty request payload")
	}

	if strings.TrimSpace(req.UserId) == "" {
		return nil, status.Error(codes.InvalidArgument, "missing user_id in event")
	}

	if strings.TrimSpace(req.EventType) == "" {
		return nil, status.Error(codes.InvalidArgument, "missing event_type in event")
	}

	// Poison pill protection: Prevent out-of-range floats from crashing PostgreSQL DECIMAL(10,2)
	if math.IsNaN(req.Value) || math.IsInf(req.Value, 0) || math.Abs(req.Value) > 99999999.99 {
		return nil, status.Error(codes.InvalidArgument, "value out of supported numeric range [-99999999.99, 99999999.99]")
	}

	// Preserve caller-provided idempotency/trace ID (critical for PostgreSQL ON CONFLICT deduplication
	// across Kafka replays). Only fall back to OTel span TraceID or random ID when absent.
	span := trace.SpanFromContext(ctx)
	var otelTraceID string
	if span.SpanContext().IsValid() {
		tid := span.SpanContext().TraceID().String()
		if tid != "" && tid != "00000000000000000000000000000000" {
			otelTraceID = tid
		}
	}

	traceID := strings.TrimSpace(req.TraceId)
	if traceID == "" {
		if otelTraceID != "" {
			traceID = otelTraceID
		} else {
			traceID = generateFallbackTraceID()
		}
	}

	// Serialize per-user Read -> Evaluate -> Persist -> Commit to eliminate concurrent
	// rolling-window baseline races for the same user_id.
	unlock, err := s.acquireUserLock(ctx, req.UserId)
	if err != nil {
		return nil, err
	}
	defer unlock()

	// 1. Hot-state cache: Read rolling average BEFORE persistence.
	// Never mutates Redis state prior to DB confirmation, preventing dual-write duplication on retries.
	// Returns nil if Redis is unavailable or on user cold start.
	rollingAvg := s.hotState.ComputeAverage(ctx, req.UserId, traceID)

	// 2. Anomaly detection: Two-tier evaluation against pre-existing window
	isAnomaly := DetermineAnomaly(req.Value, rollingAvg, s.config.DeviationMultiplier, s.config.FallbackThreshold)

	// 3. PostgreSQL persistence: Durable audit record with retry and idempotency
	var eventTime time.Time
	if req.Timestamp > 0 {
		eventTime = time.Unix(req.Timestamp, 0).UTC()
	} else {
		eventTime = time.Now().UTC()
	}

	record := &ProcessedEventRecord{
		UserID:         req.UserId,
		EventType:      req.EventType,
		EventValue:     req.Value,
		RollingAvg:     rollingAvg,
		IsAnomaly:      isAnomaly,
		TraceID:        traceID,
		EventTimestamp: eventTime,
	}

	var inserted bool
	var dbErr error
	backoff := 50 * time.Millisecond
	maxAttempts := 3

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		inserted, dbErr = s.repo.SaveProcessedEvent(ctx, record)
		if dbErr == nil {
			break
		}

		s.logger.WarnContext(ctx, "failed to persist processed event to database, retrying",
			"attempt", attempt,
			"max_attempts", maxAttempts,
			"user_id", req.UserId,
			"trace_id", traceID,
			"error", dbErr,
		)

		if attempt < maxAttempts {
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, status.Errorf(codes.Canceled, "context canceled during db retry: %v", ctx.Err())
			case <-timer.C:
				backoff *= 2
			}
		}
	}

	if dbErr != nil {
		s.logger.ErrorContext(ctx, "failed to persist processed event after retries; returning gRPC error to prevent offset commit",
			"user_id", req.UserId,
			"trace_id", traceID,
			"error", dbErr,
		)
		return nil, status.Errorf(codes.Unavailable, "database persistence failed: %v", dbErr)
	}

	// 4. Redis Hot State Commit: Executed ONLY after successful database write and ONLY if inserted (prevents replay contamination)
	if inserted {
		var commitErr error
		commitBackoff := 50 * time.Millisecond
	commitLoop:
		for attempt := 1; attempt <= 3; attempt++ {
			commitErr = s.hotState.CommitEvent(ctx, req.UserId, req.Value, isAnomaly, traceID)
			if commitErr == nil {
				break
			}
			if attempt < 3 {
				timer := time.NewTimer(commitBackoff)
				select {
				case <-ctx.Done():
					timer.Stop()
					break commitLoop
				case <-timer.C:
					commitBackoff *= 2
				}
			}
		}

		if commitErr != nil {
			s.logger.WarnContext(ctx, "failed to commit event to redis hot state window after retries (audit record safely in postgres)",
				"user_id", req.UserId,
				"trace_id", traceID,
				"error", commitErr,
			)
		}
	} else {
		s.logger.InfoContext(ctx, "skipping redis hot state commit and alert dispatch for duplicate replayed event",
			"user_id", req.UserId,
			"trace_id", traceID,
		)
	}

	// 5. Notification trigger: Asynchronously queue alert ONLY for newly inserted anomalies (prevents duplicate alerts on replay)
	if inserted && isAnomaly && s.notificationClient != nil {
		select {
		case <-s.stopWorkers:
			s.logger.WarnContext(ctx, "processor stopping, skipping notification enqueue",
				"user_id", req.UserId,
				"trace_id", traceID,
			)
		default:
			alertReq := &eventpb.AlertRequest{
				UserId:     req.UserId,
				EventType:  req.EventType,
				EventValue: req.Value,
				IsAnomaly:  true,
				TraceId:    traceID,
				Timestamp:  eventTime.Unix(),
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
					"trace_id", traceID,
				)
			}
		}
	}

	logAttrs := []any{
		"trace_id", traceID,
		"user_id", req.UserId,
		"event_type", req.EventType,
		"value", req.Value,
		"rolling_avg", rollingAvg,
		"is_anomaly", isAnomaly,
		"inserted", inserted,
	}
	if otelTraceID != "" {
		logAttrs = append(logAttrs, "otel_trace_id", otelTraceID)
	}
	s.logger.InfoContext(ctx, "event processed", logAttrs...)

	return &eventpb.ProcessResponse{
		Success:     true,
		Message:     "event processed successfully",
		ProcessedAt: time.Now().Unix(),
		IsAnomaly:   isAnomaly,
	}, nil
}
