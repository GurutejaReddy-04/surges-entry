package main

import (
	"context"
	"io"
	"log/slog"
	"testing"
)

func TestRedisHotStateStore_UnreachableGracefulDegradation(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	// Unreachable Redis address to test graceful degradation error branch
	store := NewRedisHotStateStore("127.0.0.1:54321", 10, logger)
	defer store.Close()

	avg := store.UpdateAndComputeAverage(context.Background(), "user-test", 100.0, "trace-test")
	if avg != nil {
		t.Errorf("expected nil rolling average on unreachable redis, got %v", *avg)
	}

	// Test decoupled ComputeAverage on unreachable Redis
	avg2 := store.ComputeAverage(context.Background(), "user-test", "trace-test")
	if avg2 != nil {
		t.Errorf("expected nil rolling average on unreachable redis from ComputeAverage, got %v", *avg2)
	}

	// Test CommitEvent normal on unreachable Redis returns error
	errNormal := store.CommitEvent(context.Background(), "user-test", 100.0, false, "trace-test")
	if errNormal == nil {
		t.Errorf("expected error from CommitEvent on unreachable redis, got nil")
	}

	// Test CommitEvent anomaly (TTL renewal) on unreachable Redis returns error
	errAnomaly := store.CommitEvent(context.Background(), "user-test", 100.0, true, "trace-test")
	if errAnomaly == nil {
		t.Errorf("expected error from CommitEvent (anomaly) on unreachable redis, got nil")
	}
}

func TestComputeAverageFromStrings(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	ctx := context.Background()

	tests := []struct {
		name     string
		vals     []string
		expected *float64
	}{
		{
			name:     "empty slice returns nil",
			vals:     []string{},
			expected: nil,
		},
		{
			name: "valid floats calculate correct mean",
			vals: []string{"10.5", "20.5"},
			expected: func() *float64 {
				v := 15.5
				return &v
			}(),
		},
		{
			name: "skips malformed strings and averages remainder",
			vals: []string{"invalid", "30.0"},
			expected: func() *float64 {
				v := 30.0
				return &v
			}(),
		},
		{
			name:     "all malformed strings returns nil",
			vals:     []string{"bad1", "bad2"},
			expected: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := computeAverageFromStrings(tc.vals, logger, ctx, "user-1", "trace-1")
			if tc.expected == nil {
				if result != nil {
					t.Fatalf("expected nil, got %v", *result)
				}
			} else {
				if result == nil {
					t.Fatalf("expected %v, got nil", *tc.expected)
				}
				if *result != *tc.expected {
					t.Fatalf("expected %v, got %v", *tc.expected, *result)
				}
			}
		})
	}
}
