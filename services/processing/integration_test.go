//go:build integration

package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	eventpb "surges-entry/proto/gen/event"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func integrationLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

type recordingNotificationServer struct {
	eventpb.UnimplementedNotificationServiceServer
	receivedAlerts chan *eventpb.AlertRequest
}

func (s *recordingNotificationServer) SendAlert(ctx context.Context, req *eventpb.AlertRequest) (*eventpb.AlertResponse, error) {
	s.receivedAlerts <- req
	return &eventpb.AlertResponse{
		Success: true,
		Message: "alert recorded",
	}, nil
}

func TestIntegration_RedisTxPipelineAndWindowEvolution(t *testing.T) {
	cfg := LoadConfig()
	ctx := context.Background()

	// 1. Verify Redis is reachable
	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis unavailable at %s: %v", cfg.RedisAddr, err)
	}
	defer rdb.Close()

	hotState := NewRedisHotStateStore(cfg.RedisAddr, 10, integrationLogger())
	defer hotState.Close()

	userID := fmt.Sprintf("integ-user-%d", time.Now().UnixNano())
	key := fmt.Sprintf("window:%s", userID)
	defer rdb.Del(ctx, key)

	// Feed 3 values: 10, 20, 30.
	// UpdateAndComputeAverage executes LRANGE before LPUSH (pre-write read) to prevent outlier self-pollution.
	// Prior to appending 30, the historical window contains [10, 20], yielding (10 + 20) / 2 = 15.0.
	vals := []float64{10.0, 20.0, 30.0}
	var lastAvg *float64
	for _, v := range vals {
		lastAvg = hotState.UpdateAndComputeAverage(ctx, userID, v, "trace-window-test")
	}

	if lastAvg == nil {
		t.Fatalf("expected non-nil rolling average")
	}
	if *lastAvg != 15.0 {
		t.Errorf("expected pre-write rolling average 15.0, got %f", *lastAvg)
	}

	// Verify post-commit rolling average across all 3 items (10, 20, 30) is 20.0
	committedAvg := hotState.ComputeAverage(ctx, userID, "trace-window-verify")
	if committedAvg == nil || *committedAvg != 20.0 {
		t.Errorf("expected committed rolling average 20.0, got %v", committedAvg)
	}

	// Verify TTL was set on the key
	ttl, err := rdb.TTL(ctx, key).Result()
	if err != nil {
		t.Fatalf("failed to query TTL: %v", err)
	}
	if ttl <= 0 || ttl > 3600*time.Second {
		t.Errorf("expected TTL up to 3600s, got %v", ttl)
	}

	// Feed 15 values to test LTRIM capping at 10
	for i := 1; i <= 15; i++ {
		hotState.UpdateAndComputeAverage(ctx, userID, 100.0, "trace-cap-test")
	}

	listLen, err := rdb.LLen(ctx, key).Result()
	if err != nil {
		t.Fatalf("failed to get list length: %v", err)
	}
	if listLen != 10 {
		t.Errorf("expected list length capped at 10, got %d", listLen)
	}
}

func TestIntegration_LivePipeline_Redis_Postgres_Notification(t *testing.T) {
	cfg := LoadConfig()
	ctx := context.Background()

	// 1. Check Redis & Postgres
	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis not running: %v", err)
	}
	rdb.Close()

	pool, err := pgxpool.New(ctx, cfg.PostgresDSN)
	if err != nil {
		t.Skipf("Postgres pool init failed: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("Postgres ping failed: %v", err)
	}
	defer pool.Close()

	// 2. Start mock Notification Service on dynamic port
	notifLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on dynamic port: %v", err)
	}
	defer notifLis.Close()

	notifServer := grpc.NewServer()
	recNotif := &recordingNotificationServer{
		receivedAlerts: make(chan *eventpb.AlertRequest, 10),
	}
	eventpb.RegisterNotificationServiceServer(notifServer, recNotif)
	go func() { _ = notifServer.Serve(notifLis) }()
	defer notifServer.Stop()

	notifConn, err := grpc.NewClient(
		notifLis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to dial notification server: %v", err)
	}
	defer notifConn.Close()
	notifClient := eventpb.NewNotificationServiceClient(notifConn)

	// 3. Setup real components
	hotState := NewRedisHotStateStore(cfg.RedisAddr, cfg.RedisWindowSize, integrationLogger())
	defer hotState.Close()

	repo, err := NewPostgresEventRepository(ctx, cfg.PostgresDSN, integrationLogger())
	if err != nil {
		t.Fatalf("failed to create repo: %v", err)
	}
	defer repo.Close()

	processor := NewEventProcessor(hotState, repo, notifClient, cfg, integrationLogger())

	userID := fmt.Sprintf("integ-flow-%d", time.Now().UnixNano())
	defer func() {
		cleanupRdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
		cleanupRdb.Del(ctx, fmt.Sprintf("window:%s", userID))
		cleanupRdb.Close()
	}()

	// Feed baseline events: 5 events at 100.0 (rolling avg = 100.0)
	for i := 0; i < 5; i++ {
		_, err := processor.ProcessEvent(ctx, &eventpb.Event{
			UserId:    userID,
			EventType: "baseline",
			Value:     100.0,
			Timestamp: time.Now().Unix(),
			TraceId:   fmt.Sprintf("trace-baseline-%d", i),
		})
		if err != nil {
			t.Fatalf("baseline event %d failed: %v", i, err)
		}
	}

	// Verify no alerts dispatched during baseline
	if len(recNotif.receivedAlerts) != 0 {
		t.Errorf("expected 0 alerts during baseline, got %d", len(recNotif.receivedAlerts))
	}

	// Feed anomalous event: value 200.0 > 100.0 * 1.5 (150.0)
	anomalyTraceID := fmt.Sprintf("trace-anomaly-%d", time.Now().UnixNano())
	res, err := processor.ProcessEvent(ctx, &eventpb.Event{
		UserId:    userID,
		EventType: "spike",
		Value:     200.0,
		Timestamp: time.Now().Unix(),
		TraceId:   anomalyTraceID,
	})
	if err != nil {
		t.Fatalf("process anomalous event failed: %v", err)
	}

	if !res.Success {
		t.Errorf("expected success=true")
	}
	if !res.IsAnomaly {
		t.Errorf("expected is_anomaly=true for spike value 200.0")
	}

	// Verify Notification Service received the alert
	select {
	case alert := <-recNotif.receivedAlerts:
		if alert.UserId != userID || alert.EventValue != 200.0 || alert.TraceId != anomalyTraceID {
			t.Errorf("notification alert mismatch: %+v", alert)
		}
		t.Logf("verified alert received by Notification Service: %+v", alert)
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for anomaly alert to reach Notification Service")
	}

	// 4. Verify record was durably written to PostgreSQL
	var (
		dbUserID     string
		dbEventType  string
		dbValue      float64
		dbRollingAvg *float64
		dbIsAnomaly  bool
		dbTraceID    string
	)

	query := `
		SELECT user_id, event_type, event_value, rolling_avg, is_anomaly, trace_id
		FROM processed_events
		WHERE trace_id = $1;
	`
	row := pool.QueryRow(ctx, query, anomalyTraceID)
	err = row.Scan(&dbUserID, &dbEventType, &dbValue, &dbRollingAvg, &dbIsAnomaly, &dbTraceID)
	if err != nil {
		t.Fatalf("failed to query saved event from postgres: %v", err)
	}

	if dbUserID != userID || dbEventType != "spike" || dbValue != 200.0 {
		t.Errorf("db values mismatch: user=%s type=%s val=%f", dbUserID, dbEventType, dbValue)
	}
	if !dbIsAnomaly {
		t.Errorf("expected db is_anomaly=true")
	}
	if dbRollingAvg == nil {
		t.Errorf("expected non-nil rolling_avg in db")
	} else {
		t.Logf("verified postgres row: user=%s, value=%.2f, rolling_avg=%.2f, is_anomaly=%v, trace=%s",
			dbUserID, dbValue, *dbRollingAvg, dbIsAnomaly, dbTraceID)
	}
}

func TestIntegration_GracefulDegradation_WhenRedisIsDown(t *testing.T) {
	cfg := LoadConfig()
	ctx := context.Background()

	// Check Postgres
	pool, err := pgxpool.New(ctx, cfg.PostgresDSN)
	if err != nil {
		t.Skipf("Postgres pool init failed: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("Postgres ping failed: %v", err)
	}
	defer pool.Close()

	repo, err := NewPostgresEventRepository(ctx, cfg.PostgresDSN, integrationLogger())
	if err != nil {
		t.Fatalf("failed to create repo: %v", err)
	}
	defer repo.Close()

	// Point Redis to an unreachable port to simulate outage
	deadRedisStore := NewRedisHotStateStore("127.0.0.1:59999", 10, integrationLogger())
	defer deadRedisStore.Close()

	mockNotif := &mockNotificationClient{}
	processor := NewEventProcessor(deadRedisStore, repo, mockNotif, cfg, integrationLogger())

	// Event 1: Value 150.0 < fallback threshold 1000.0 -> is_anomaly = false
	traceNormal := fmt.Sprintf("trace-degrade-norm-%d", time.Now().UnixNano())
	resNormal, err := processor.ProcessEvent(ctx, &eventpb.Event{
		UserId:    "user-degraded",
		EventType: "transaction",
		Value:     150.0,
		Timestamp: time.Now().Unix(),
		TraceId:   traceNormal,
	})
	if err != nil {
		t.Fatalf("expected gRPC call to succeed despite Redis failure, got %v", err)
	}
	if resNormal.IsAnomaly {
		t.Errorf("expected is_anomaly=false for value 150.0 under fallback threshold")
	}

	// Event 2: Value 1500.0 > fallback threshold 1000.0 -> is_anomaly = true
	traceAnomaly := fmt.Sprintf("trace-degrade-anom-%d", time.Now().UnixNano())
	resAnomaly, err := processor.ProcessEvent(ctx, &eventpb.Event{
		UserId:    "user-degraded",
		EventType: "transaction",
		Value:     1500.0,
		Timestamp: time.Now().Unix(),
		TraceId:   traceAnomaly,
	})
	if err != nil {
		t.Fatalf("expected gRPC call to succeed, got %v", err)
	}
	if !resAnomaly.IsAnomaly {
		t.Errorf("expected is_anomaly=true for value 1500.0 exceeding fallback threshold")
	}

	// Verify alert dispatched for the fallback anomaly
	if len(mockNotif.sentAlerts) != 1 {
		t.Errorf("expected 1 alert dispatched for fallback anomaly, got %d", len(mockNotif.sentAlerts))
	}

	// Verify both rows in Postgres have rolling_avg IS NULL
	var countNull int
	checkQuery := `
		SELECT COUNT(*) FROM processed_events
		WHERE trace_id IN ($1, $2) AND rolling_avg IS NULL;
	`
	err = pool.QueryRow(ctx, checkQuery, traceNormal, traceAnomaly).Scan(&countNull)
	if err != nil {
		t.Fatalf("failed to query degraded events in postgres: %v", err)
	}
	if countNull != 2 {
		t.Errorf("expected 2 events with NULL rolling_avg in postgres, got %d", countNull)
	}
	t.Logf("successfully verified graceful degradation: 2 events persisted with NULL rolling_avg and fallback anomaly detection")
}
