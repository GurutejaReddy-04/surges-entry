package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

func TestNewHealthServer(t *testing.T) {
	srv := newHealthServer("8080")
	if srv == nil {
		t.Fatal("expected non-nil server")
	}
	if srv.Addr != ":8080" {
		t.Errorf("expected addr :8080, got %s", srv.Addr)
	}

	// Test /healthz handler
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

type mockSaramaConsumerGroup struct {
	errChan      chan error
	consumeCount int
	consumeErr   error
}

func (m *mockSaramaConsumerGroup) Consume(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
	m.consumeCount++
	<-ctx.Done()
	return m.consumeErr
}

func (m *mockSaramaConsumerGroup) Errors() <-chan error {
	return m.errChan
}

func (m *mockSaramaConsumerGroup) Close() error                         { return nil }
func (m *mockSaramaConsumerGroup) Pause(partitions map[string][]int32)  {}
func (m *mockSaramaConsumerGroup) Resume(partitions map[string][]int32) {}
func (m *mockSaramaConsumerGroup) PauseAll()                            {}
func (m *mockSaramaConsumerGroup) ResumeAll()                           {}

func TestRunConsumerLoops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cg := &mockSaramaConsumerGroup{
		errChan: make(chan error, 2),
	}
	cg.errChan <- errors.New("sample error")

	handler := NewIngestionHandler(&mockProcessingClient{}, testLogger(), time.Second)
	wg := runConsumerLoops(ctx, cg, "events", handler, testLogger())

	time.Sleep(50 * time.Millisecond)
	cancel()
	wg.Wait()

	if cg.consumeCount == 0 {
		t.Errorf("expected consume to be called at least once")
	}
}

func TestNewHealthServer_WithLivenessCheck(t *testing.T) {
	isLive := false
	srv := newHealthServer("8080", func() bool { return isLive })
	if srv == nil {
		t.Fatal("expected non-nil server")
	}

	// 1. Should return 503 when unhealthy
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("expected status 503 when isLive is false, got %d", rec.Code)
	}

	// 2. Should return 200 when healthy
	isLive = true
	rec2 := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec2, req)
	if rec2.Code != http.StatusOK {
		t.Errorf("expected status 200 when isLive is true, got %d", rec2.Code)
	}
}

func TestInitGRPCClient(t *testing.T) {
	conn, err := initGRPCClient("localhost:50051")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if conn == nil {
		t.Fatal("expected non-nil grpc conn")
	}
	defer conn.Close()
}

func TestInitConsumerGroup_Error(t *testing.T) {
	cfg := newSaramaConfig()
	// An empty broker list should fail
	_, err := initConsumerGroup([]string{}, "group", cfg)
	if err == nil {
		t.Error("expected error with empty brokers, got nil")
	}
}

func TestRun_InvalidConsumerGroup(t *testing.T) {
	cfg := Config{
		KafkaBrokers:     []string{},
		ConsumerGroupID:  "test-group",
		ProcessingTarget: "localhost:50051",
		GRPCTimeout:      time.Second,
	}

	err := run(context.Background(), cfg, testLogger())
	if err == nil {
		t.Error("expected error from run with invalid brokers, got nil")
	}
}

type failingConsumerGroup struct {
	errChan chan error
	calls   atomic.Int32
	err     error
}

func (f *failingConsumerGroup) Consume(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
	f.calls.Add(1)
	return f.err
}
func (f *failingConsumerGroup) Errors() <-chan error               { return f.errChan }
func (f *failingConsumerGroup) Close() error                       { return nil }
func (f *failingConsumerGroup) Pause(map[string][]int32)           {}
func (f *failingConsumerGroup) Resume(map[string][]int32)          {}
func (f *failingConsumerGroup) PauseAll()                          {}
func (f *failingConsumerGroup) ResumeAll()                         {}

func TestRunConsumerLoops_BackoffAndLivenessDegradation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cg := &failingConsumerGroup{
		errChan: make(chan error, 1),
		err:     errors.New("broker unavailable"),
	}

	var isLive atomic.Bool
	isLive.Store(true)

	handler := NewIngestionHandler(&mockProcessingClient{}, testLogger(), time.Second)
	wg := runConsumerLoops(ctx, cg, "events", handler, testLogger(), &isLive)

	time.Sleep(900 * time.Millisecond)
	cancel()
	wg.Wait()

	calls := cg.calls.Load()
	if calls < 2 || calls > 6 {
		t.Fatalf("expected exponential backoff to bound retries (2..6 calls in 900ms), got %d", calls)
	}
	if isLive.Load() {
		t.Fatal("expected isLive to flip false after consecutive consume errors / loop exit")
	}
}

func TestRunConsumerLoops_ExitsCleanlyOnClosedConsumerGroup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cg := &failingConsumerGroup{
		errChan: make(chan error, 1),
		err:     sarama.ErrClosedConsumerGroup,
	}

	var isLive atomic.Bool
	isLive.Store(true)

	handler := NewIngestionHandler(&mockProcessingClient{}, testLogger(), time.Second)
	wg := runConsumerLoops(ctx, cg, "events", handler, testLogger(), &isLive)

	time.Sleep(50 * time.Millisecond)
	cancel()
	wg.Wait()

	if cg.calls.Load() != 1 {
		t.Fatalf("expected exactly 1 Consume call before exiting on ErrClosedConsumerGroup, got %d", cg.calls.Load())
	}
	if isLive.Load() {
		t.Fatal("expected isLive=false after consumer loop exits")
	}
}
