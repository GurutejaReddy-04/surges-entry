package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"time"

	eventpb "surges-entry/proto/gen/event"

	"github.com/IBM/sarama"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ProcessingClient interface {
	ProcessEvent(ctx context.Context, in *eventpb.Event, opts ...grpc.CallOption) (*eventpb.ProcessResponse, error)
}

type IngestionHandler struct {
	client    ProcessingClient
	logger    *slog.Logger
	timeout   time.Duration
	onHealthy func()
}

func NewIngestionHandler(client ProcessingClient, logger *slog.Logger, timeout time.Duration) *IngestionHandler {
	return &IngestionHandler{
		client:  client,
		logger:  logger,
		timeout: timeout,
	}
}

// deterministicKafkaTraceID derives a stable 32-hex-char idempotency key from immutable
// Kafka message coordinates so crash-replays and offset rewinds produce the exact same
// key even when the upstream producer omits trace_id.
func deterministicKafkaTraceID(msg *sarama.ConsumerMessage) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s:%d:%d", msg.Topic, msg.Partition, msg.Offset)))
	return hex.EncodeToString(sum[:16])
}

func (h *IngestionHandler) Setup(session sarama.ConsumerGroupSession) error {
	h.logger.Info("kafka consumer group session setup initiated",
		"member_id", session.MemberID(),
		"generation_id", session.GenerationID(),
		"claims", session.Claims(),
	)
	if h.onHealthy != nil {
		h.onHealthy()
	}
	return nil
}

func (h *IngestionHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	h.logger.Info("kafka consumer group session cleanup initiated",
		"member_id", session.MemberID(),
		"generation_id", session.GenerationID(),
	)
	return nil
}

func (h *IngestionHandler) ProcessSingleMessage(ctx context.Context, msg *sarama.ConsumerMessage) error {
	fallbackID := deterministicKafkaTraceID(msg)
	event, err := ParseAndValidateEventWithFallback(msg.Value, fallbackID)
	if err != nil {
		h.logger.WarnContext(ctx, "failed to parse or validate event; discarding malformed message",
			"partition", msg.Partition,
			"offset", msg.Offset,
			"raw_payload", string(msg.Value),
			"error", err,
		)
		// Return nil so unparseable malformed events don't stall the partition consumer loop forever
		return nil
	}

	tracer := otel.Tracer("ingestion-service")
	spanCtx, span := tracer.Start(ctx, "ingestion.consume_event",
		trace.WithAttributes(
			attribute.String("user_id", event.UserId),
			attribute.String("event_type", event.EventType),
			attribute.String("event.idempotency_key", event.TraceId),
			attribute.Int("partition", int(msg.Partition)),
			attribute.Int64("offset", msg.Offset),
		),
	)
	defer span.End()

	// Preserve deterministic event.TraceId across replays; never overwrite with an ephemeral
	// per-attempt OTel span TraceID (which would bypass PostgreSQL ON CONFLICT deduplication).
	// W3C traceparent headers still propagate the OTel span context over gRPC automatically.

	// Retry logic with exponential backoff (3 attempts: 50ms, 100ms, 200ms) for transient gRPC forwarding failures
	var forwardErr error
	var resp *eventpb.ProcessResponse
	backoff := 50 * time.Millisecond
	maxAttempts := 3

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		rpcCtx, cancel := context.WithTimeout(spanCtx, h.timeout)
		resp, forwardErr = h.client.ProcessEvent(rpcCtx, event)
		cancel()

		if forwardErr == nil && resp != nil && resp.Success {
			break
		}

		if forwardErr == nil && resp == nil {
			forwardErr = errors.New("processing service returned nil response")
		} else if forwardErr == nil && !resp.Success {
			forwardErr = fmt.Errorf("processing service rejected event: %s", resp.Message)
		}

		// Poison pill defense: If downstream explicitly rejected the request as invalid,
		// retrying will never succeed. Drop the poison pill and continue partition consumption.
		if forwardErr != nil {
			st, ok := status.FromError(forwardErr)
			if ok && st.Code() == codes.InvalidArgument {
				h.logger.WarnContext(spanCtx, "dropping poison pill (invalid argument, unrecoverable)",
					"partition", msg.Partition,
					"offset", msg.Offset,
					"user_id", event.UserId,
					"error", forwardErr,
				)
				return nil // Drop and continue
			}
		}

		h.logger.WarnContext(spanCtx, "forward to processing service failed, retrying",
			"attempt", attempt,
			"max_attempts", maxAttempts,
			"backoff_ms", backoff.Milliseconds(),
			"user_id", event.UserId,
			"partition", msg.Partition,
			"offset", msg.Offset,
			"error", forwardErr,
		)

		if attempt < maxAttempts {
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return fmt.Errorf("context canceled during retry: %w", ctx.Err())
			case <-timer.C:
				backoff *= 2
			}
		}
	}

	if forwardErr != nil {
		return fmt.Errorf("forward to processing service failed after %d attempts: %w", maxAttempts, forwardErr)
	}

	h.logger.InfoContext(spanCtx, "event successfully forwarded to processing service",
		"trace_id", event.TraceId,
		"user_id", event.UserId,
		"event_type", event.EventType,
		"value", event.Value,
		"partition", msg.Partition,
		"offset", msg.Offset,
		"processed_at", resp.ProcessedAt,
	)

	return nil
}

func (h *IngestionHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		if session.Context().Err() != nil {
			return nil
		}

		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}

			// Process message with retries. If unrecoverable, return error to halt partition claim
			// WITHOUT marking the message offset, strictly maintaining at-least-once delivery guarantees.
			if err := h.ProcessSingleMessage(session.Context(), msg); err != nil {
				h.logger.ErrorContext(session.Context(), "unrecoverable message processing failure; halting partition consumption",
					"partition", msg.Partition,
					"offset", msg.Offset,
					"error", err,
				)
				return fmt.Errorf("halting partition consumption on unrecoverable failure: %w", err)
			}

			// Explicit at-least-once offset mark ONLY after processing succeeds
			session.MarkMessage(msg, "")

		case <-session.Context().Done():
			return nil
		}
	}
}
