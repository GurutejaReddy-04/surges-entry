package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	eventpb "event-platform/proto/gen/event"

	"github.com/IBM/sarama"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type mockProcessingClient struct {
	mu           sync.Mutex
	calledEvents []*eventpb.Event
	returnError  error
	returnResp   *eventpb.ProcessResponse
}

func (m *mockProcessingClient) ProcessEvent(ctx context.Context, in *eventpb.Event, opts ...grpc.CallOption) (*eventpb.ProcessResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.calledEvents = append(m.calledEvents, in)
	if m.returnError != nil {
		return nil, m.returnError
	}
	if m.returnResp != nil {
		return m.returnResp, nil
	}
	return &eventpb.ProcessResponse{
		Success:     true,
		Message:     "ok",
		ProcessedAt: time.Now().Unix(),
	}, nil
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

func TestProcessSingleMessage_Success(t *testing.T) {
	mockClient := &mockProcessingClient{}
	handler := NewIngestionHandler(mockClient, testLogger(), 2*time.Second)

	rawJSON := `{"user_id":"alice","event_type":"click","value":150.5,"timestamp":1234567890}`
	msg := &sarama.ConsumerMessage{
		Topic:     "events",
		Partition: 2,
		Offset:    42,
		Key:       []byte("alice"),
		Value:     []byte(rawJSON),
	}

	err := handler.ProcessSingleMessage(context.Background(), msg)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(mockClient.calledEvents) != 1 {
		t.Fatalf("expected 1 call to gRPC client, got %d", len(mockClient.calledEvents))
	}

	call := mockClient.calledEvents[0]
	if call.UserId != "alice" {
		t.Errorf("expected user_id alice, got %s", call.UserId)
	}
	if call.EventType != "click" {
		t.Errorf("expected event_type click, got %s", call.EventType)
	}
	if call.Value != 150.5 {
		t.Errorf("expected value 150.5, got %f", call.Value)
	}
	if call.Timestamp != 1234567890 {
		t.Errorf("expected timestamp 1234567890, got %d", call.Timestamp)
	}
	if call.TraceId == "" {
		t.Errorf("expected auto-generated trace_id, got empty")
	}
}

func TestProcessSingleMessage_SimulatorRFC3339Timestamp(t *testing.T) {
	mockClient := &mockProcessingClient{}
	handler := NewIngestionHandler(mockClient, testLogger(), 2*time.Second)

	rawJSON := `{"user_id":"user-003","event_type":"transaction","value":99.5,"timestamp":"2026-09-06T22:46:31Z","trace_id":"custom-trace"}`
	msg := &sarama.ConsumerMessage{
		Topic:     "events",
		Partition: 1,
		Offset:    10,
		Key:       []byte("user-003"),
		Value:     []byte(rawJSON),
	}

	err := handler.ProcessSingleMessage(context.Background(), msg)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(mockClient.calledEvents) != 1 {
		t.Fatalf("expected 1 call, got %d", len(mockClient.calledEvents))
	}

	call := mockClient.calledEvents[0]
	if call.TraceId != "custom-trace" {
		t.Errorf("expected custom-trace, got %s", call.TraceId)
	}
	if call.Timestamp <= 0 {
		t.Errorf("expected valid parsed unix timestamp, got %d", call.Timestamp)
	}
}

func TestProcessSingleMessage_MalformedJSONDropped(t *testing.T) {
	mockClient := &mockProcessingClient{}
	handler := NewIngestionHandler(mockClient, testLogger(), 2*time.Second)

	msg := &sarama.ConsumerMessage{
		Topic: "events",
		Value: []byte(`not-json`),
	}

	// Should drop cleanly and not return an error that retries forever
	err := handler.ProcessSingleMessage(context.Background(), msg)
	if err != nil {
		t.Errorf("expected nil error on dropped malformed event, got %v", err)
	}

	if len(mockClient.calledEvents) != 0 {
		t.Errorf("expected 0 gRPC calls for malformed event, got %d", len(mockClient.calledEvents))
	}
}

func TestProcessSingleMessage_GRPCErrorsPropagatedForRetry(t *testing.T) {
	mockClient := &mockProcessingClient{
		returnError: errors.New("connection refused"),
	}
	handler := NewIngestionHandler(mockClient, testLogger(), 2*time.Second)

	rawJSON := `{"user_id":"bob","event_type":"purchase","value":25.0,"timestamp":123456}`
	msg := &sarama.ConsumerMessage{
		Topic:     "events",
		Partition: 0,
		Offset:    100,
		Value:     []byte(rawJSON),
	}

	err := handler.ProcessSingleMessage(context.Background(), msg)
	if err == nil {
		t.Fatal("expected error when gRPC call fails, got nil")
	}
}

func TestProcessSingleMessage_PoisonPillDropped(t *testing.T) {
	mockClient := &mockProcessingClient{
		returnError: status.Error(codes.InvalidArgument, "value out of supported numeric range [-99999999.99, 99999999.99]"),
	}
	handler := NewIngestionHandler(mockClient, testLogger(), 2*time.Second)

	rawJSON := `{"user_id":"bad-val","event_type":"purchase","value":50.0,"timestamp":123456}`
	msg := &sarama.ConsumerMessage{
		Topic:     "events",
		Partition: 1,
		Offset:    200,
		Value:     []byte(rawJSON),
	}

	// Poison pills rejected with codes.InvalidArgument must be dropped (return nil error)
	// so the partition loop can mark the message and continue without wedging the consumer.
	err := handler.ProcessSingleMessage(context.Background(), msg)
	if err != nil {
		t.Fatalf("expected nil error for dropped poison pill, got %v", err)
	}
}

func TestPoisonPill_Value100Million_DroppedWithoutStall(t *testing.T) {
	mockClient := &mockProcessingClient{}
	handler := NewIngestionHandler(mockClient, testLogger(), 2*time.Second)

	// Direct poison pill message as specified in Stage 5: value 100000000.0
	rawJSON := `{"user_id":"poison-test","event_type":"overflow","value":100000000.0,"timestamp":1234567890}`
	msg := &sarama.ConsumerMessage{
		Topic:     "events",
		Partition: 2,
		Offset:    500,
		Value:     []byte(rawJSON),
	}

	// Must be dropped without partition stall (err == nil)
	err := handler.ProcessSingleMessage(context.Background(), msg)
	if err != nil {
		t.Fatalf("expected nil error for 100M overflow poison pill, got %v", err)
	}

	// Downstream gRPC client should never even be reached because parser rejected it
	if len(mockClient.calledEvents) != 0 {
		t.Errorf("expected 0 gRPC calls for 100M poison pill, got %d", len(mockClient.calledEvents))
	}
}

type mockConsumerGroupSession struct {
	ctx        context.Context
	markedMsgs []*sarama.ConsumerMessage
	mu         sync.Mutex
}

func (m *mockConsumerGroupSession) Claims() map[string][]int32 {
	return map[string][]int32{"events": {0, 1}}
}
func (m *mockConsumerGroupSession) MemberID() string    { return "member-1" }
func (m *mockConsumerGroupSession) GenerationID() int32 { return 1 }
func (m *mockConsumerGroupSession) Context() context.Context {
	if m.ctx != nil {
		return m.ctx
	}
	return context.Background()
}
func (m *mockConsumerGroupSession) MarkOffset(topic string, partition int32, offset int64, metadata string) {
}
func (m *mockConsumerGroupSession) ResetOffset(topic string, partition int32, offset int64, metadata string) {
}
func (m *mockConsumerGroupSession) MarkMessage(msg *sarama.ConsumerMessage, metadata string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.markedMsgs = append(m.markedMsgs, msg)
}
func (m *mockConsumerGroupSession) Commit() {}

type mockConsumerGroupClaim struct {
	msgs chan *sarama.ConsumerMessage
}

func (m *mockConsumerGroupClaim) Topic() string                            { return "events" }
func (m *mockConsumerGroupClaim) Partition() int32                         { return 0 }
func (m *mockConsumerGroupClaim) InitialOffset() int64                     { return 0 }
func (m *mockConsumerGroupClaim) HighWaterMarkOffset() int64               { return 100 }
func (m *mockConsumerGroupClaim) Messages() <-chan *sarama.ConsumerMessage { return m.msgs }

func TestSetupAndCleanup(t *testing.T) {
	handler := NewIngestionHandler(&mockProcessingClient{}, testLogger(), time.Second)
	sess := &mockConsumerGroupSession{}

	if err := handler.Setup(sess); err != nil {
		t.Errorf("expected nil error on Setup, got %v", err)
	}
	if err := handler.Cleanup(sess); err != nil {
		t.Errorf("expected nil error on Cleanup, got %v", err)
	}
}

func TestProcessSingleMessage_RetrySuccess(t *testing.T) {
	var attempts int
	mockClient := &mockProcessingClient{}
	handler := NewIngestionHandler(mockClient, testLogger(), time.Second)

	// Override ProcessEvent to fail first attempt, succeed on second
	customClient := &retryMockClient{
		failCount: 1,
	}
	handler.client = customClient

	rawJSON := `{"user_id":"charlie","event_type":"login","value":10.0,"timestamp":123456}`
	msg := &sarama.ConsumerMessage{
		Topic: "events",
		Value: []byte(rawJSON),
	}

	err := handler.ProcessSingleMessage(context.Background(), msg)
	if err != nil {
		t.Fatalf("expected success after retry, got %v", err)
	}
	if customClient.totalCalls != 2 {
		t.Errorf("expected 2 calls, got %d", customClient.totalCalls)
	}
	_ = attempts
}

type retryMockClient struct {
	mu         sync.Mutex
	failCount  int
	totalCalls int
}

func (m *retryMockClient) ProcessEvent(ctx context.Context, in *eventpb.Event, opts ...grpc.CallOption) (*eventpb.ProcessResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.totalCalls++
	if m.totalCalls <= m.failCount {
		return nil, errors.New("transient network blip")
	}
	return &eventpb.ProcessResponse{
		Success:     true,
		Message:     "ok",
		ProcessedAt: time.Now().Unix(),
	}, nil
}

func TestConsumeClaim_SuccessMarksMessage(t *testing.T) {
	mockClient := &mockProcessingClient{}
	handler := NewIngestionHandler(mockClient, testLogger(), time.Second)

	sess := &mockConsumerGroupSession{}
	claim := &mockConsumerGroupClaim{
		msgs: make(chan *sarama.ConsumerMessage, 1),
	}

	msg := &sarama.ConsumerMessage{
		Topic:     "events",
		Partition: 0,
		Offset:    5,
		Value:     []byte(`{"user_id":"u1","event_type":"view","value":1.0,"timestamp":100}`),
	}
	claim.msgs <- msg
	close(claim.msgs)

	err := handler.ConsumeClaim(sess, claim)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(sess.markedMsgs) != 1 {
		t.Fatalf("expected 1 marked message, got %d", len(sess.markedMsgs))
	}
	if sess.markedMsgs[0].Offset != 5 {
		t.Errorf("expected marked offset 5, got %d", sess.markedMsgs[0].Offset)
	}
}

func TestConsumeClaim_FailureHaltsPartitionWithoutMarking(t *testing.T) {
	mockClient := &mockProcessingClient{
		returnError: errors.New("persistent gRPC failure"),
	}
	handler := NewIngestionHandler(mockClient, testLogger(), 50*time.Millisecond)

	sess := &mockConsumerGroupSession{}
	claim := &mockConsumerGroupClaim{
		msgs: make(chan *sarama.ConsumerMessage, 1),
	}

	msg := &sarama.ConsumerMessage{
		Topic:     "events",
		Partition: 3,
		Offset:    99,
		Value:     []byte(`{"user_id":"u2","event_type":"view","value":1.0,"timestamp":100}`),
	}
	claim.msgs <- msg

	// ConsumeClaim should return error upon exhausting retries
	err := handler.ConsumeClaim(sess, claim)
	if err == nil {
		t.Fatal("expected ConsumeClaim to return error and halt partition, got nil")
	}

	// CRITICAL: Message MUST NOT be marked as committed!
	if len(sess.markedMsgs) != 0 {
		t.Errorf("critical bug: failed message was marked as committed! got %d marked messages", len(sess.markedMsgs))
	}
}
