package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	eventpb "surges-entry/proto/gen/event"
)

type rawEvent struct {
	UserID    string          `json:"user_id"`
	EventType string          `json:"event_type"`
	Value     float64         `json:"value"`
	Timestamp json.RawMessage `json:"timestamp"`
	TraceID   string          `json:"trace_id"`
}

func generateTraceID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%032x", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

func parseTimestamp(raw json.RawMessage) int64 {
	if len(raw) == 0 {
		return time.Now().Unix()
	}

	str := strings.TrimSpace(strings.Trim(string(raw), "\""))
	if str == "" || str == "null" {
		return time.Now().Unix()
	}

	if n, err := strconv.ParseInt(str, 10, 64); err == nil {
		// Normalize nanoseconds, microseconds, or milliseconds to Unix seconds
		if n > 1e16 { // nanoseconds
			return n / 1e9
		}
		if n > 1e14 { // microseconds
			return n / 1e6
		}
		if n > 1e11 { // milliseconds
			return n / 1e3
		}
		return n
	}

	// Try RFC3339 / RFC3339Nano (e.g. from simulator)
	if t, err := time.Parse(time.RFC3339Nano, str); err == nil {
		return t.Unix()
	}
	if t, err := time.Parse(time.RFC3339, str); err == nil {
		return t.Unix()
	}

	return time.Now().Unix()
}

// ParseAndValidateEvent parses and validates a JSON event payload, generating a
// random fallback trace_id if the payload does not include one.
func ParseAndValidateEvent(data []byte) (*eventpb.Event, error) {
	return ParseAndValidateEventWithFallback(data, "")
}

// ParseAndValidateEventWithFallback parses and validates a JSON event payload.
// When the payload omits trace_id, fallbackTraceID (if non-empty) is used so
// Kafka consumers can supply a deterministic idempotency key derived from
// (topic, partition, offset) that remains stable across replays.
func ParseAndValidateEventWithFallback(data []byte, fallbackTraceID string) (*eventpb.Event, error) {
	if len(data) == 0 {
		return nil, errors.New("event payload is empty")
	}

	var raw rawEvent
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("malformed event json: %w", err)
	}

	if strings.TrimSpace(raw.UserID) == "" {
		return nil, errors.New("event validation failed: missing user_id")
	}

	if strings.TrimSpace(raw.EventType) == "" {
		return nil, errors.New("event validation failed: missing event_type")
	}

	// Poison pill protection: Prevent out-of-range floats from causing PostgreSQL DECIMAL(10,2) overflow
	if math.IsNaN(raw.Value) || math.IsInf(raw.Value, 0) || math.Abs(raw.Value) > 99999999.99 {
		return nil, fmt.Errorf("event validation failed: value %v out of supported numeric range [-99999999.99, 99999999.99]", raw.Value)
	}

	traceID := strings.TrimSpace(raw.TraceID)
	if traceID == "" {
		if fb := strings.TrimSpace(fallbackTraceID); fb != "" {
			traceID = fb
		} else {
			traceID = generateTraceID()
		}
	}

	ts := parseTimestamp(raw.Timestamp)

	return &eventpb.Event{
		UserId:    raw.UserID,
		EventType: raw.EventType,
		Value:     raw.Value,
		Timestamp: ts,
		TraceId:   traceID,
	}, nil
}
