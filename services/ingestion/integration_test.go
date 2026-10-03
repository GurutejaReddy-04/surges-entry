//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"

	eventpb "surges-entry/proto/gen/event"

	"github.com/IBM/sarama"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type recordingProcessorServer struct {
	eventpb.UnimplementedProcessingServiceServer
	receivedEvents chan *eventpb.Event
}

func (s *recordingProcessorServer) ProcessEvent(ctx context.Context, req *eventpb.Event) (*eventpb.ProcessResponse, error) {
	s.receivedEvents <- req
	return &eventpb.ProcessResponse{
		Success:     true,
		Message:     "integration event processed",
		ProcessedAt: time.Now().Unix(),
	}, nil
}

func TestEndToEndPipeline_KafkaToIngestionToProcessing(t *testing.T) {
	broker := getEnv("KAFKA_BROKER", "localhost:9092")

	// 1. Check if Kafka broker is reachable
	client, err := sarama.NewClient([]string{broker}, sarama.NewConfig())
	if err != nil {
		t.Skipf("Kafka is not running at %s: %v", broker, err)
	}
	client.Close()

	// 2. Start mock Processing gRPC server on a dynamic port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on dynamic port: %v", err)
	}
	defer listener.Close()

	grpcServer := grpc.NewServer()
	mockProc := &recordingProcessorServer{
		receivedEvents: make(chan *eventpb.Event, 10),
	}
	eventpb.RegisterProcessingServiceServer(grpcServer, mockProc)

	go func() {
		_ = grpcServer.Serve(listener)
	}()
	defer grpcServer.Stop()

	// 3. Connect gRPC client to mock processing server
	conn, err := grpc.NewClient(
		listener.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to dial mock processing server: %v", err)
	}
	defer conn.Close()

	procClient := eventpb.NewProcessingServiceClient(conn)
	handler := NewIngestionHandler(procClient, testLogger(), 5*time.Second)

	// 4. Publish a test event to Kafka
	testTopic := "events"
	uniqueUserID := fmt.Sprintf("integration-user-%d", time.Now().UnixNano())
	payload := map[string]any{
		"user_id":    uniqueUserID,
		"event_type": "transaction",
		"value":      299.95,
		"timestamp":  time.Now().Unix(),
		"trace_id":   "integ-trace-xyz",
	}
	rawBytes, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	saramaCfg := sarama.NewConfig()
	saramaCfg.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer([]string{broker}, saramaCfg)
	if err != nil {
		t.Fatalf("failed to create sync producer: %v", err)
	}
	defer producer.Close()

	partition, offset, err := producer.SendMessage(&sarama.ProducerMessage{
		Topic: testTopic,
		Key:   sarama.StringEncoder(uniqueUserID),
		Value: sarama.ByteEncoder(rawBytes),
	})
	if err != nil {
		t.Fatalf("failed to produce test message: %v", err)
	}
	t.Logf("produced message to %s (partition %d, offset %d)", testTopic, partition, offset)

	// 5. Start Ingestion consumer group with a unique group ID
	cgConfig := newSaramaConfig()
	cgConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	groupID := fmt.Sprintf("integ-group-%d", time.Now().UnixNano())

	consumerGroup, err := sarama.NewConsumerGroup([]string{broker}, groupID, cgConfig)
	if err != nil {
		t.Fatalf("failed to create consumer group: %v", err)
	}
	defer consumerGroup.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	go func() {
		for {
			if err := consumerGroup.Consume(ctx, []string{testTopic}, handler); err != nil {
				return
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()

	// 6. Await processing receipt
	for {
		select {
		case ev := <-mockProc.receivedEvents:
			if ev.UserId == uniqueUserID {
				if ev.EventType != "transaction" {
					t.Errorf("expected event_type transaction, got %s", ev.EventType)
				}
				if ev.Value != 299.95 {
					t.Errorf("expected value 299.95, got %f", ev.Value)
				}
				if ev.TraceId != "integ-trace-xyz" {
					t.Errorf("expected trace_id integ-trace-xyz, got %s", ev.TraceId)
				}
				t.Logf("verified event received by Processing gRPC server: %+v", ev)
				return
			}
		case <-ctx.Done():
			t.Fatalf("timed out waiting for event with user_id %s to arrive at processing server", uniqueUserID)
		}
	}
}
