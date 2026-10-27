// Package main provides the SurgesEntry Ingestion Service.
// This service consumes events from Kafka and forwards them to the Processing Service via gRPC.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	eventpb "surges-entry/proto/gen/event"

	"github.com/IBM/sarama"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

const internalAuthHeader = "x-surges-internal-token"

func authUnaryClientInterceptor() grpc.UnaryClientInterceptor {
	token := os.Getenv("INTERNAL_AUTH_TOKEN")
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if token != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, internalAuthHeader, token)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func newSaramaConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategyRoundRobin(),
	}
	return config
}

func newHealthServer(port string, isHealthy ...func() bool) *http.Server {
	healthMux := http.NewServeMux()
	healthMux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if len(isHealthy) > 0 && isHealthy[0] != nil && !isHealthy[0]() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("unhealthy"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	return &http.Server{Addr: ":" + port, Handler: healthMux}
}

func runConsumerLoops(ctx context.Context, cg sarama.ConsumerGroup, topic string, handler sarama.ConsumerGroupHandler, logger *slog.Logger) *sync.WaitGroup {
	var wg sync.WaitGroup

	// Consumer error listener with graceful context termination
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case err, ok := <-cg.Errors():
				if !ok {
					return
				}
				logger.Error("consumer group error", "error", err)
			}
		}
	}()

	// Ingestion consumption loop
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if err := cg.Consume(ctx, []string{topic}, handler); err != nil {
				if ctx.Err() != nil {
					return
				}
				logger.Error("error during partition consumption", "error", err)
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()

	return &wg
}

func initGRPCClient(target string) (*grpc.ClientConn, error) {
	return grpc.NewClient(
		target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
		grpc.WithUnaryInterceptor(authUnaryClientInterceptor()),
		grpc.WithDefaultServiceConfig(`{"loadBalancingConfig": [{"round_robin":{}}]}`),
	)
}

func initConsumerGroup(brokers []string, groupID string, cfg *sarama.Config) (sarama.ConsumerGroup, error) {
	return sarama.NewConsumerGroup(brokers, groupID, cfg)
}

func run(ctx context.Context, cfg Config, logger *slog.Logger) error {
	// Initialize OpenTelemetry TracerProvider
	shutdownTracer, err := InitTelemetry(ctx, "ingestion-service", logger)
	if err != nil {
		logger.Warn("failed to initialize tracer provider; running without telemetry", "error", err)
	} else {
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = shutdownTracer(shutdownCtx)
		}()
	}

	// Establish gRPC connection to Processing Service with OpenTelemetry client stats handler
	grpcConn, err := initGRPCClient(cfg.ProcessingTarget)
	if err != nil {
		return fmt.Errorf("failed to create grpc client connection: %w", err)
	}

	processingClient := eventpb.NewProcessingServiceClient(grpcConn)
	handler := NewIngestionHandler(processingClient, logger, cfg.GRPCTimeout)

	// Initialize Kafka Consumer Group
	saramaCfg := newSaramaConfig()
	consumerGroup, err := initConsumerGroup(cfg.KafkaBrokers, cfg.ConsumerGroupID, saramaCfg)
	if err != nil {
		_ = grpcConn.Close()
		return fmt.Errorf("failed to create sarama consumer group: %w", err)
	}

	var isConsumerLive atomic.Bool
	isConsumerLive.Store(true)

	// Lightweight HTTP health probe on port 8080 for container health checks
	healthPort := getEnv("HEALTH_PORT", "8080")
	httpHealthServer := newHealthServer(healthPort, isConsumerLive.Load)
	go func() {
		if err := httpHealthServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("http health check server error", "error", err)
		}
	}()
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpHealthServer.Shutdown(shutdownCtx)
	}()

	wg := runConsumerLoops(ctx, consumerGroup, cfg.KafkaTopic, handler, logger)

	logger.Info("ingestion service is actively consuming messages", "health_port", healthPort)
	<-ctx.Done()

	logger.Info("shutting down ingestion service gracefully")
	isConsumerLive.Store(false)

	// Clean shutdown sequence:
	// 1. Close consumer group to stop receiving new Kafka messages and signal cg.Consume to exit
	_ = consumerGroup.Close()

	// 2. Wait for consumer loops to exit cleanly, ensuring in-flight messages finish processing
	wg.Wait()

	// 3. Now safe to close gRPC client connection
	_ = grpcConn.Close()

	return nil
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	cfg := LoadConfig()

	logger.Info("starting ingestion service",
		"kafka_brokers", cfg.KafkaBrokers,
		"topic", cfg.KafkaTopic,
		"group_id", cfg.ConsumerGroupID,
		"processing_target", cfg.ProcessingTarget,
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg, logger); err != nil {
		logger.Error("ingestion service failed", "error", err)
		os.Exit(1)
	}
}
