package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type ProcessedEventRecord struct {
	UserID         string
	EventType      string
	EventValue     float64
	RollingAvg     *float64
	IsAnomaly      bool
	TraceID        string
	EventTimestamp time.Time
}

type EventRepository interface {
	SaveProcessedEvent(ctx context.Context, record *ProcessedEventRecord) (bool, error)
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

func (r *PostgresEventRepository) SaveProcessedEvent(ctx context.Context, record *ProcessedEventRecord) (bool, error) {
	if r.pool == nil {
		return false, fmt.Errorf("postgres connection pool is not initialized")
	}

	eventTS := record.EventTimestamp
	if eventTS.IsZero() {
		eventTS = time.Now()
	}

	query := `
		INSERT INTO processed_events (
			user_id,
			event_type,
			event_value,
			rolling_avg,
			is_anomaly,
			trace_id,
			event_timestamp,
			ingested_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
		ON CONFLICT (trace_id) WHERE trace_id IS NOT NULL AND trace_id <> '' DO NOTHING
		RETURNING id;
	`

	var id int64
	err := r.pool.QueryRow(ctx, query,
		record.UserID,
		record.EventType,
		record.EventValue,
		record.RollingAvg,
		record.IsAnomaly,
		record.TraceID,
		eventTS,
	).Scan(&id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Duplicate event dropped cleanly via ON CONFLICT DO NOTHING
			return false, nil
		}
		return false, fmt.Errorf("db insert failed: %w", err)
	}

	return true, nil
}
