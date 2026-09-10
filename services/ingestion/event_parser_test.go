package main

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestGenerateTraceID(t *testing.T) {
	traceID := generateTraceID()
	if len(traceID) != 32 {
		t.Fatalf("expected 32 hex characters for W3C trace ID, got length %d (%s)", len(traceID), traceID)
	}

	traceID2 := generateTraceID()
	if traceID == traceID2 {
		t.Errorf("expected unique trace IDs, got identical: %s", traceID)
	}
}

func TestParseTimestamp(t *testing.T) {
	now := time.Now().Unix()

	// Empty
	if ts := parseTimestamp(json.RawMessage("")); ts < now-1 || ts > now+1 {
		t.Errorf("expected current timestamp for empty rawMessage, got %d", ts)
	}

	// Unix seconds as string
	if ts := parseTimestamp(json.RawMessage(`"1700000000"`)); ts != 1700000000 {
		t.Errorf("expected 1700000000, got %d", ts)
	}

	// Unix seconds as int
	if ts := parseTimestamp(json.RawMessage(`1700000000`)); ts != 1700000000 {
		t.Errorf("expected 1700000000, got %d", ts)
	}

	// Milliseconds normalization
	if ts := parseTimestamp(json.RawMessage(`1700000000000`)); ts != 1700000000 {
		t.Errorf("expected normalized seconds from ms, got %d", ts)
	}

	// Nanoseconds normalization
	if ts := parseTimestamp(json.RawMessage(`1700000000000000000`)); ts != 1700000000 {
		t.Errorf("expected normalized seconds from ns, got %d", ts)
	}

	// RFC3339Nano
	rfcTime := "2026-09-07T12:34:56.789Z"
	parsed, _ := time.Parse(time.RFC3339Nano, rfcTime)
	if ts := parseTimestamp(json.RawMessage(`"` + rfcTime + `"`)); ts != parsed.Unix() {
		t.Errorf("expected RFC3339 timestamp %d, got %d", parsed.Unix(), ts)
	}

	// RFC3339 standard
	rfcStandard := "2026-09-07T12:34:56Z"
	parsedStd, _ := time.Parse(time.RFC3339, rfcStandard)
	if ts := parseTimestamp(json.RawMessage(`"` + rfcStandard + `"`)); ts != parsedStd.Unix() {
		t.Errorf("expected RFC3339 standard %d, got %d", parsedStd.Unix(), ts)
	}

	// Invalid string fallback
	if ts := parseTimestamp(json.RawMessage(`"not-a-timestamp"`)); ts < now-1 || ts > now+1 {
		t.Errorf("expected current timestamp on invalid string, got %d", ts)
	}
}

func TestParseAndValidateEvent(t *testing.T) {
	// Empty payload
	if _, err := ParseAndValidateEvent(nil); err == nil {
		t.Error("expected error on empty payload, got nil")
	}

	// Invalid JSON
	if _, err := ParseAndValidateEvent([]byte(`{invalid}`)); err == nil {
		t.Error("expected error on malformed json, got nil")
	}

	// Missing user_id
	noUser := `{"event_type":"login","value":10.0}`
	if _, err := ParseAndValidateEvent([]byte(noUser)); err == nil || !strings.Contains(err.Error(), "missing user_id") {
		t.Errorf("expected missing user_id error, got %v", err)
	}

	// Whitespace user_id
	blankUser := `{"user_id":"   ","event_type":"login","value":10.0}`
	if _, err := ParseAndValidateEvent([]byte(blankUser)); err == nil || !strings.Contains(err.Error(), "missing user_id") {
		t.Errorf("expected missing user_id error, got %v", err)
	}

	// Missing event_type
	noType := `{"user_id":"u-1","value":10.0}`
	if _, err := ParseAndValidateEvent([]byte(noType)); err == nil || !strings.Contains(err.Error(), "missing event_type") {
		t.Errorf("expected missing event_type error, got %v", err)
	}

	// Valid payload with existing trace_id
	valid := `{"user_id":"u-1","event_type":"purchase","value":99.99,"timestamp":1700000000,"trace_id":"trace-123"}`
	ev, err := ParseAndValidateEvent([]byte(valid))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev.UserId != "u-1" || ev.EventType != "purchase" || ev.Value != 99.99 || ev.TraceId != "trace-123" {
		t.Errorf("event fields mismatch: %+v", ev)
	}

	// Valid payload without trace_id (auto-generates 32-hex)
	validNoTrace := `{"user_id":"u-2","event_type":"login","value":5.0}`
	ev2, err := ParseAndValidateEvent([]byte(validNoTrace))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ev2.TraceId) != 32 {
		t.Errorf("expected auto-generated 32-char trace_id, got %s (len %d)", ev2.TraceId, len(ev2.TraceId))
	}

	// Poison pill protection: Numeric bounds [-99999999.99, 99999999.99]
	overflow := `{"user_id":"u-3","event_type":"overflow","value":100000000.0}`
	if _, err := ParseAndValidateEvent([]byte(overflow)); err == nil || !strings.Contains(err.Error(), "out of supported numeric range") {
		t.Errorf("expected numeric range error for value >= 1e8, got %v", err)
	}

	underflow := `{"user_id":"u-3","event_type":"underflow","value":-100000000.0}`
	if _, err := ParseAndValidateEvent([]byte(underflow)); err == nil || !strings.Contains(err.Error(), "out of supported numeric range") {
		t.Errorf("expected numeric range error for value <= -1e8, got %v", err)
	}

	// Max allowable boundary values
	maxVal := `{"user_id":"u-3","event_type":"max","value":99999999.99}`
	if evMax, err := ParseAndValidateEvent([]byte(maxVal)); err != nil || evMax.Value != 99999999.99 {
		t.Errorf("expected success for boundary 99999999.99, got err=%v, ev=%v", err, evMax)
	}
}
