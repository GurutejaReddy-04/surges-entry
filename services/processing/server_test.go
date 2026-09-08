package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math"
	"sync"
	"testing"
	"time"

	eventpb "surges-entry/proto/gen/event"

	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

type mockHotStateStore struct {
	mu               sync.Mutex
	returnAvg        *float64
	calledUser       string
	calledValue      float64
	calledTrace      string
	invocations      int
	committedUser    string
	committedValue   float64
	committedAnomaly bool
	committedCount   int
	commitErr        error
}

func (m *mockHotStateStore) ComputeAverage(ctx context.Context, userID string, traceID string) *float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.invocations++
	m.calledUser = userID
	m.calledTrace = traceID
	return m.returnAvg
}

func (m *mockHotStateStore) CommitEvent(ctx context.Context, userID string, value float64, isAnomaly bool, traceID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.committedCount++
	m.committedUser = userID
	m.committedValue = value
	m.committedAnomaly = isAnomaly
	return m.commitErr
}

func (m *mockHotStateStore) UpdateAndComputeAverage(ctx context.Context, userID string, value float64, traceID string) *float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.invocations++
	m.calledUser = userID
	m.calledValue = value
	m.calledTrace = traceID
	return m.returnAvg
}

func (m *mockHotStateStore) Close() error {
	return nil
}

type mockEventRepository struct {
	mu          sync.Mutex
	savedRecord *ProcessedEventRecord
	returnErr   error
}

func (m *mockEventRepository) SaveProcessedEvent(ctx context.Context, record *ProcessedEventRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.savedRecord = record
	return m.returnErr
}

func (m *mockEventRepository) Close() {}

type mockNotificationClient struct {
	mu         sync.Mutex
	sentAlerts []*eventpb.AlertRequest
	sentCtxs   []context.Context
	returnErr  error
}

func (m *mockNotificationClient) SendAlert(ctx context.Context, in *eventpb.AlertRequest, opts ...grpc.CallOption) (*eventpb.AlertResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sentAlerts = append(m.sentAlerts, in)
	m.sentCtxs = append(m.sentCtxs, ctx)
	if m.returnErr != nil {
		return nil, m.returnErr
	}
	return &eventpb.AlertResponse{
		Success: true,
		Message: "alert received",
	}, nil
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

func testConfig() Config {
	return Config{
		FallbackThreshold:   1000.0,
		DeviationMultiplier: 1.5,
		RedisWindowSize:     10,
	}
}

func TestProcessEvent_SuccessWithRollingAvgAndNotification(t *testing.T) {
	avg := 50.0
	mockHotState := &mockHotStateStore{returnAvg: &avg}
	mockRepo := &mockEventRepository{}
	mockNotif := &mockNotificationClient{}

	processor := NewEventProcessor(mockHotState, mockRepo, mockNotif, testConfig(), testLogger())
	defer processor.Stop()

	// 1. Non-anomalous event: value 60 <= 50.0 * 1.5 (75.0) -> No alert
	reqNormal := &eventpb.Event{
		UserId:    "user-100",
		EventType: "transaction",
		Value:     60.0,
		Timestamp: time.Now().Unix(),
		TraceId:   "trace-normal-1",
	}

	resNormal, err := processor.ProcessEvent(context.Background(), reqNormal)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resNormal.Success {
		t.Errorf("expected success=true")
	}
	if resNormal.IsAnomaly {
		t.Errorf("expected is_anomaly=false for normal value")
	}

	// Wait briefly for worker queue check
	time.Sleep(50 * time.Millisecond)
	mockNotif.mu.Lock()
	if len(mockNotif.sentAlerts) != 0 {
		t.Errorf("expected 0 alerts sent for normal event, got %d", len(mockNotif.sentAlerts))
	}
	mockNotif.mu.Unlock()

	// Verify CommitEvent was called with isAnomaly=false
	if mockHotState.committedAnomaly {
		t.Errorf("expected committedAnomaly=false for normal value")
	}

	// 2. Anomalous event: value 80 > 50.0 * 1.5 (75.0) -> Alert queued
	reqAnomaly := &eventpb.Event{
		UserId:    "user-100",
		EventType: "transaction",
		Value:     80.0,
		Timestamp: time.Now().Unix(),
		TraceId:   "trace-anomaly-1",
	}

	resAnomaly, err := processor.ProcessEvent(context.Background(), reqAnomaly)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resAnomaly.IsAnomaly {
		t.Errorf("expected is_anomaly=true for anomaly value")
	}

	// Verify CommitEvent was called with isAnomaly=true (outlier filtering)
	if !mockHotState.committedAnomaly {
		t.Errorf("expected committedAnomaly=true for anomaly value")
	}

	// Wait for background worker to dispatch
	time.Sleep(100 * time.Millisecond)
	mockNotif.mu.Lock()
	defer mockNotif.mu.Unlock()
	if len(mockNotif.sentAlerts) != 1 {
		t.Fatalf("expected 1 alert sent for anomaly, got %d", len(mockNotif.sentAlerts))
	}
	alert := mockNotif.sentAlerts[0]
	if alert.UserId != "user-100" || alert.EventValue != 80.0 || alert.TraceId != "trace-anomaly-1" {
		t.Errorf("unexpected alert payload: %+v", alert)
	}
}

func TestProcessEvent_NotificationFailureDoesNotFailGRPC(t *testing.T) {
	avg := 50.0
	mockHotState := &mockHotStateStore{returnAvg: &avg}
	mockRepo := &mockEventRepository{}
	mockNotif := &mockNotificationClient{
		returnErr: errors.New("notification service connection refused"),
	}

	processor := NewEventProcessor(mockHotState, mockRepo, mockNotif, testConfig(), testLogger())
	defer processor.Stop()

	reqAnomaly := &eventpb.Event{
		UserId:    "user-101",
		EventType: "transaction",
		Value:     85.0, // Anomaly
		Timestamp: time.Now().Unix(),
		TraceId:   "trace-notif-fail",
	}

	res, err := processor.ProcessEvent(context.Background(), reqAnomaly)
	if err != nil {
		t.Fatalf("expected gRPC call to succeed even if notification fails, got %v", err)
	}
	if !res.Success {
		t.Errorf("expected success=true")
	}

	// Allow worker time to attempt dispatch and log warning
	time.Sleep(50 * time.Millisecond)
}

func TestProcessEvent_GracefulDegradationOnRedisFailure(t *testing.T) {
	mockHotState := &mockHotStateStore{returnAvg: nil} // nil simulates Redis down
	mockRepo := &mockEventRepository{}
	mockNotif := &mockNotificationClient{}

	processor := NewEventProcessor(mockHotState, mockRepo, mockNotif, testConfig(), testLogger())
	defer processor.Stop()

	// Value 1500.0 > FallbackThreshold (1000.0) -> Fallback detects anomaly
	reqFallbackAnomaly := &eventpb.Event{
		UserId:    "user-102",
		EventType: "payment",
		Value:     1500.0,
		Timestamp: time.Now().Unix(),
		TraceId:   "trace-degraded-anomaly",
	}

	res, err := processor.ProcessEvent(context.Background(), reqFallbackAnomaly)
	if err != nil {
		t.Fatalf("expected success under degraded Redis mode, got %v", err)
	}
	if !res.IsAnomaly {
		t.Errorf("expected fallback logic to detect anomaly for 1500.0 > 1000.0")
	}

	// Verify DB record persisted rolling_avg = nil
	mockRepo.mu.Lock()
	defer mockRepo.mu.Unlock()
	if mockRepo.savedRecord == nil {
		t.Fatal("expected saved record in repository")
	}
	if mockRepo.savedRecord.RollingAvg != nil {
		t.Errorf("expected rolling_avg to be nil in DB when Redis fails, got %v", *mockRepo.savedRecord.RollingAvg)
	}
}

func TestProcessEvent_DatabaseFailureReturnsGRPCError(t *testing.T) {
	avg := 50.0
	mockHotState := &mockHotStateStore{returnAvg: &avg}
	mockRepo := &mockEventRepository{
		returnErr: errors.New("connection to PostgreSQL pool timed out"),
	}
	mockNotif := &mockNotificationClient{}

	processor := NewEventProcessor(mockHotState, mockRepo, mockNotif, testConfig(), testLogger())
	defer processor.Stop()

	req := &eventpb.Event{
		UserId:    "user-103",
		EventType: "click",
		Value:     10.0,
		Timestamp: time.Now().Unix(),
		TraceId:   "trace-db-err",
	}

	res, err := processor.ProcessEvent(context.Background(), req)
	if err == nil {
		t.Fatal("expected gRPC error when DB fails after retries, got nil")
	}
	if res != nil {
		t.Errorf("expected nil response on error, got %v", res)
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %v", err)
	}
	if st.Code() != codes.Unavailable {
		t.Errorf("expected code Unavailable, got %v", st.Code())
	}

	// Dual-write consistency verification: When DB fails, Redis CommitEvent is NEVER called!
	if mockHotState.committedCount != 0 {
		t.Errorf("expected 0 Redis commits when DB fails, got %d", mockHotState.committedCount)
	}
}

func TestProcessEvent_ValidationErrors(t *testing.T) {
	processor := NewEventProcessor(&mockHotStateStore{}, &mockEventRepository{}, &mockNotificationClient{}, testConfig(), testLogger())
	defer processor.Stop()

	tests := []struct {
		name string
		req  *eventpb.Event
	}{
		{
			name: "nil request",
			req:  nil,
		},
		{
			name: "missing user_id",
			req: &eventpb.Event{
				UserId:    "",
				EventType: "click",
				Value:     10.0,
				Timestamp: time.Now().Unix(),
			},
		},
		{
			name: "numeric overflow poison pill (value >= 1e8)",
			req: &eventpb.Event{
				UserId:    "u-overflow",
				EventType: "click",
				Value:     100000000.0,
				Timestamp: time.Now().Unix(),
			},
		},
		{
			name: "numeric underflow poison pill (value <= -1e8)",
			req: &eventpb.Event{
				UserId:    "u-underflow",
				EventType: "click",
				Value:     -100000000.0,
				Timestamp: time.Now().Unix(),
			},
		},
		{
			name: "NaN value poison pill",
			req: &eventpb.Event{
				UserId:    "u-nan",
				EventType: "click",
				Value:     math.NaN(),
				Timestamp: time.Now().Unix(),
			},
		},
		{
			name: "Inf value poison pill",
			req: &eventpb.Event{
				UserId:    "u-inf",
				EventType: "click",
				Value:     math.Inf(1),
				Timestamp: time.Now().Unix(),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := processor.ProcessEvent(context.Background(), tt.req)
			if err == nil {
				t.Fatalf("expected error for %s, got nil", tt.name)
			}
			st, ok := status.FromError(err)
			if !ok || st.Code() != codes.InvalidArgument {
				t.Errorf("expected code InvalidArgument for %s, got %v", tt.name, st.Code())
			}
		})
	}
}

func TestProcessEvent_WorkerTraceContextPropagation(t *testing.T) {
	avg := 50.0
	mockHotState := &mockHotStateStore{returnAvg: &avg}
	mockRepo := &mockEventRepository{}
	mockNotif := &mockNotificationClient{}

	processor := NewEventProcessor(mockHotState, mockRepo, mockNotif, testConfig(), testLogger())
	defer processor.Stop()

	// Inject active span into context
	traceIDHex := "4bf92f3577b34da6a3ce929d0e0e4736"
	traceID, _ := trace.TraceIDFromHex(traceIDHex)
	spanCtx := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     trace.SpanID{1, 2, 3, 4, 5, 6, 7, 8},
		TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), spanCtx)

	reqAnomaly := &eventpb.Event{
		UserId:    "user-otel-test",
		EventType: "transaction",
		Value:     150.0, // Anomaly > 50*1.5
		Timestamp: time.Now().Unix(),
		TraceId:   traceIDHex,
	}

	_, err := processor.ProcessEvent(ctx, reqAnomaly)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Allow worker time to execute
	time.Sleep(100 * time.Millisecond)

	mockNotif.mu.Lock()
	defer mockNotif.mu.Unlock()
	if len(mockNotif.sentCtxs) != 1 {
		t.Fatalf("expected 1 sent alert context, got %d", len(mockNotif.sentCtxs))
	}

	// Verify worker context preserved the parent OpenTelemetry span
	workerSpan := trace.SpanFromContext(mockNotif.sentCtxs[0])
	if workerSpan.SpanContext().TraceID().String() != traceIDHex {
		t.Errorf("expected worker trace ID %s, got %s", traceIDHex, workerSpan.SpanContext().TraceID().String())
	}
}

func TestHealthCheck(t *testing.T) {
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	healthServer.SetServingStatus(eventpb.ProcessingService_ServiceDesc.ServiceName, grpc_health_v1.HealthCheckResponse_SERVING)

	resp, err := healthServer.Check(context.Background(), &grpc_health_v1.HealthCheckRequest{
		Service: eventpb.ProcessingService_ServiceDesc.ServiceName,
	})
	if err != nil {
		t.Fatalf("health check failed: %v", err)
	}

	if resp.Status != grpc_health_v1.HealthCheckResponse_SERVING {
		t.Errorf("expected status SERVING, got %v", resp.Status)
	}
}

func TestProcessEvent_AlertQueueFull(t *testing.T) {
	avg := 50.0
	mockHotState := &mockHotStateStore{returnAvg: &avg}
	mockRepo := &mockEventRepository{}
	mockNotif := &mockNotificationClient{}

	processor := NewEventProcessor(mockHotState, mockRepo, mockNotif, testConfig(), testLogger())
	defer processor.Stop()

	// Fill the alertQueue completely
	for i := 0; i < cap(processor.alertQueue); i++ {
		processor.alertQueue <- &alertTask{req: &eventpb.AlertRequest{UserId: "dummy"}}
	}

	// Trigger another anomaly when queue is full: must NOT block or fail gRPC call
	reqAnomaly := &eventpb.Event{
		UserId:    "user-full",
		EventType: "transaction",
		Value:     100.0, // Anomaly
		Timestamp: time.Now().Unix(),
		TraceId:   "trace-full-queue",
	}

	res, err := processor.ProcessEvent(context.Background(), reqAnomaly)
	if err != nil {
		t.Fatalf("expected no error when alert queue is full, got %v", err)
	}
	if !res.Success {
		t.Errorf("expected success=true")
	}
}

type memoryHotStateStore struct {
	window []float64
}

func (m *memoryHotStateStore) ComputeAverage(ctx context.Context, userID string, traceID string) *float64 {
	if len(m.window) == 0 {
		return nil
	}
	sum := 0.0
	for _, v := range m.window {
		sum += v
	}
	avg := sum / float64(len(m.window))
	return &avg
}

func (m *memoryHotStateStore) CommitEvent(ctx context.Context, userID string, value float64, isAnomaly bool, traceID string) error {
	if !isAnomaly {
		m.window = append(m.window, value)
	}
	return nil
}

func (m *memoryHotStateStore) UpdateAndComputeAverage(ctx context.Context, userID string, value float64, traceID string) *float64 {
	return nil
}

func (m *memoryHotStateStore) Close() error {
	return nil
}

func TestEdgeCase_OutlierFiltering_SubsequentEventsCleanBaseline(t *testing.T) {
	memStore := &memoryHotStateStore{window: make([]float64, 0)}
	mockRepo := &mockEventRepository{}
	mockNotif := &mockNotificationClient{}

	processor := NewEventProcessor(memStore, mockRepo, mockNotif, testConfig(), testLogger())
	defer processor.Stop()

	// 1. First event: Outlier 5000.0 on cold start
	ev1 := &eventpb.Event{
		UserId:    "user-edge",
		EventType: "txn",
		Value:     5000.0,
		Timestamp: time.Now().Unix(),
		TraceId:   "trace-outlier-1",
	}
	res1, err := processor.ProcessEvent(context.Background(), ev1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res1.IsAnomaly {
		t.Errorf("expected first event 5000.0 to be detected as anomaly via Tier 2 fallback")
	}
	// Because it's an anomaly, outlier filtering must NOT append it to the window
	if len(memStore.window) != 0 {
		t.Errorf("expected 0 items in window after anomaly event, got %d", len(memStore.window))
	}

	// 2. Second event: Normal 100.0. Should evaluate against clean baseline (not 5000!)
	ev2 := &eventpb.Event{
		UserId:    "user-edge",
		EventType: "txn",
		Value:     100.0,
		Timestamp: time.Now().Unix(),
		TraceId:   "trace-normal-2",
	}
	res2, err := processor.ProcessEvent(context.Background(), ev2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res2.IsAnomaly {
		t.Errorf("expected 100.0 to be normal, got is_anomaly=true")
	}
	// Normal value committed to window
	if len(memStore.window) != 1 || memStore.window[0] != 100.0 {
		t.Errorf("expected window to contain [100.0], got %v", memStore.window)
	}

	// 3. Third event: Normal 110.0 (within 100 * 1.5 = 150)
	ev3 := &eventpb.Event{
		UserId:    "user-edge",
		EventType: "txn",
		Value:     110.0,
		Timestamp: time.Now().Unix(),
		TraceId:   "trace-normal-3",
	}
	res3, err := processor.ProcessEvent(context.Background(), ev3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res3.IsAnomaly {
		t.Errorf("expected 110.0 to be normal against 100.0 rolling baseline, got is_anomaly=true")
	}
	if len(memStore.window) != 2 {
		t.Errorf("expected 2 items in window, got %d", len(memStore.window))
	}
}

func TestEdgeCase_DualWrite_DBFailureLeavesRedisUnmutated(t *testing.T) {
	mockHotState := &mockHotStateStore{}
	mockRepo := &mockEventRepository{
		returnErr: errors.New("simulated postgres connection refused"),
	}
	mockNotif := &mockNotificationClient{}

	processor := NewEventProcessor(mockHotState, mockRepo, mockNotif, testConfig(), testLogger())
	defer processor.Stop()

	req := &eventpb.Event{
		UserId:    "user-db-fail",
		EventType: "txn",
		Value:     50.0,
		Timestamp: time.Now().Unix(),
		TraceId:   "trace-db-fail",
	}

	res, err := processor.ProcessEvent(context.Background(), req)
	if err == nil {
		t.Fatal("expected error on DB failure, got nil")
	}
	if res != nil {
		t.Errorf("expected nil response on error")
	}

	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Unavailable {
		t.Errorf("expected codes.Unavailable, got %v", err)
	}

	// Core Dual-Write Guarantee: Redis CommitEvent was NEVER called!
	if mockHotState.committedCount != 0 {
		t.Fatalf("DUAL WRITE VIOLATION: Redis CommitEvent was called %d times despite DB failure!", mockHotState.committedCount)
	}
}
