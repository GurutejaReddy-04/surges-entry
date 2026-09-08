package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
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
	runConsumerLoops(ctx, cg, "events", handler, testLogger())

	time.Sleep(50 * time.Millisecond)
	cancel()
	time.Sleep(50 * time.Millisecond)

	if cg.consumeCount == 0 {
		t.Errorf("expected consume to be called at least once")
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
