package main

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

type HotStateStore interface {
	ComputeAverage(ctx context.Context, userID string, traceID string) *float64
	CommitEvent(ctx context.Context, userID string, value float64, isAnomaly bool, traceID string) error
	UpdateAndComputeAverage(ctx context.Context, userID string, value float64, traceID string) *float64
	Close() error
}

type RedisHotStateStore struct {
	client     *redis.Client
	windowSize int
	logger     *slog.Logger
}

func NewRedisHotStateStore(addr string, windowSize int, logger *slog.Logger) *RedisHotStateStore {
	rdb := redis.NewClient(&redis.Options{
		Addr:         addr,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
	})

	return &RedisHotStateStore{
		client:     rdb,
		windowSize: windowSize,
		logger:     logger,
	}
}

func (s *RedisHotStateStore) Close() error {
	return s.client.Close()
}

// ComputeAverage calculates the rolling average from the Redis window.
// Returns nil if the window is empty or Redis is unavailable.
func (s *RedisHotStateStore) ComputeAverage(ctx context.Context, userID string, traceID string) *float64 {
	key := fmt.Sprintf("window:%s", userID)

	pipe := s.client.TxPipeline()
	lrangeCmd := pipe.LRange(ctx, key, 0, -1)
	pipe.Expire(ctx, key, 3600*time.Second)

	_, err := pipe.Exec(ctx)
	if err != nil {
		s.logger.WarnContext(ctx, "redis read pipeline failed; degrading gracefully to fallback mode",
			"user_id", userID,
			"trace_id", traceID,
			"error", err,
		)
		return nil
	}

	vals := lrangeCmd.Val()
	return computeAverageFromStrings(vals, s.logger, ctx, userID, traceID)
}

// CommitEvent updates Redis state after database persistence has succeeded.
// Architectural Guarantees:
//  1. Dual-write consistency: Never called if PostgreSQL fails, preventing duplicate values on retries.
//  2. Outlier baseline protection: If isAnomaly is true, the value is NOT pushed to the window,
//     preventing outliers (e.g. 5000) from corrupting the baseline average for subsequent events.
//     Only session TTL is refreshed.
func (s *RedisHotStateStore) CommitEvent(ctx context.Context, userID string, value float64, isAnomaly bool, traceID string) error {
	key := fmt.Sprintf("window:%s", userID)

	if isAnomaly {
		// Outlier filtering: Do not append anomaly to rolling window, only reissue TTL to keep active session alive
		if err := s.client.Expire(ctx, key, 3600*time.Second).Err(); err != nil {
			s.logger.WarnContext(ctx, "failed to renew ttl on anomaly event",
				"user_id", userID,
				"trace_id", traceID,
				"error", err,
			)
			return err
		}
		return nil
	}

	// Normal event: Atomic commit to rolling window
	pipe := s.client.TxPipeline()
	pipe.LPush(ctx, key, value)
	pipe.LTrim(ctx, key, 0, int64(s.windowSize-1))
	pipe.Expire(ctx, key, 3600*time.Second)

	_, err := pipe.Exec(ctx)
	if err != nil {
		s.logger.WarnContext(ctx, "failed to commit event to redis hot state window",
			"user_id", userID,
			"trace_id", traceID,
			"value", value,
			"error", err,
		)
		return err
	}

	return nil
}

// UpdateAndComputeAverage combines read and write atomically (legacy helper).
func (s *RedisHotStateStore) UpdateAndComputeAverage(ctx context.Context, userID string, value float64, traceID string) *float64 {
	key := fmt.Sprintf("window:%s", userID)

	pipe := s.client.TxPipeline()
	lrangeCmd := pipe.LRange(ctx, key, 0, -1)
	pipe.LPush(ctx, key, value)
	pipe.LTrim(ctx, key, 0, int64(s.windowSize-1))
	pipe.Expire(ctx, key, 3600*time.Second)

	_, err := pipe.Exec(ctx)
	if err != nil {
		s.logger.ErrorContext(ctx, "redis transaction failed; degrading gracefully to fallback mode",
			"user_id", userID,
			"trace_id", traceID,
			"error", err,
		)
		return nil
	}

	vals := lrangeCmd.Val()
	return computeAverageFromStrings(vals, s.logger, ctx, userID, traceID)
}

func computeAverageFromStrings(vals []string, logger *slog.Logger, ctx context.Context, userID, traceID string) *float64 {
	if len(vals) == 0 {
		return nil
	}

	var sum float64
	var count int
	for _, vStr := range vals {
		v, parseErr := strconv.ParseFloat(vStr, 64)
		if parseErr != nil {
			if logger != nil {
				logger.WarnContext(ctx, "skipping malformed cached float in redis window",
					"value_str", vStr,
					"user_id", userID,
					"trace_id", traceID,
				)
			}
			continue
		}
		sum += v
		count++
	}

	if count == 0 {
		return nil
	}

	avg := sum / float64(count)
	return &avg
}
