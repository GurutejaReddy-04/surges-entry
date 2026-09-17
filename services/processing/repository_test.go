package main

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"
)

func TestNewPostgresEventRepository_InvalidDSN(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx := context.Background()

	// Invalid DSN that fails parsing
	repo, err := NewPostgresEventRepository(ctx, "postgres://invalid:port:is:bad", logger)
	if err == nil {
		t.Error("expected parse error for invalid DSN, got nil")
	}
	if repo != nil {
		t.Error("expected nil repository on parse error")
	}
}

func TestNewPostgresEventRepository_UnreachableHost(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// Valid DSN syntax but unreachable host
	repo, err := NewPostgresEventRepository(ctx, "postgres://user:pass@127.0.0.1:54329/testdb?connect_timeout=1", logger)
	if err == nil {
		t.Error("expected connection/ping error for unreachable host, got nil")
	}
	if repo != nil {
		t.Error("expected nil repository on connection error")
	}
}

func TestPostgresEventRepository_CloseNil(t *testing.T) {
	repo := &PostgresEventRepository{pool: nil, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	// Should not panic
	repo.Close()
}

func TestPostgresEventRepository_SaveProcessedEvent_NilPool(t *testing.T) {
	repo := &PostgresEventRepository{pool: nil, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	err := repo.SaveProcessedEvent(context.Background(), &ProcessedEventRecord{
		UserID:     "user-1",
		EventType:  "login",
		EventValue: 10,
	})
	if err == nil {
		t.Error("expected error when saving to nil pool, got nil")
	}
}
