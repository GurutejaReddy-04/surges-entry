package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type ProcessedEventRecord struct {
	UserID     string
	EventType  string
	EventValue float64
	RollingAvg *float64
	IsAnomaly  bool
	TraceID    string
}

type EventRepository interface {
	SaveProcessedEvent(ctx context.Context, record *ProcessedEventRecord) error
	Close()
}

type PostgresEventRepository struct {
	pool   *pgxpool.Pool
	logger *slog.Logger
}

func NewPostgresEventRepository(ctx context.Context, dsn string, logger *slog.Logger) (*PostgresEventRepository, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse postgres dsn: %w", err)
	}

	cfg.MaxConns = 15
	cfg.MinConns = 2
	cfg.MaxConnLifetime = 30 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to postgres pool: %w", err)
	}

	// Verify connection
	pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	if err := pool.Ping(pingCtx); err != nil {
		return nil, fmt.Errorf("postgres ping check failed: %w", err)
	}

	return &PostgresEventRepository{
		pool:   pool,
		logger: logger,
	}, nil
}

func (r *PostgresEventRepository) Close() {
	if r.pool != nil {
		r.pool.Close()
	}
}

func (r *PostgresEventRepository) SaveProcessedEvent(ctx context.Context, record *ProcessedEventRecord) error {
	if r.pool == nil {
		return fmt.Errorf("postgres connection pool is not initialized")
	}

	query := `
		INSERT INTO processed_events (
			user_id,
			event_type,
			event_value,
			rolling_avg,
			is_anomaly,
			trace_id,
			ingested_at
		) VALUES ($1, $2, $3, $4, $5, $6, NOW());
	`

	_, err := r.pool.Exec(ctx, query,
		record.UserID,
		record.EventType,
		record.EventValue,
		record.RollingAvg,
		record.IsAnomaly,
		record.TraceID,
	)
	if err != nil {
		return fmt.Errorf("db insert failed: %w", err)
	}

	return nil
}
