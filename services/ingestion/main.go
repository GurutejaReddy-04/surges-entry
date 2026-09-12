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
	"syscall"

	eventpb "surges-entry/proto/gen/event"

	"github.com/IBM/sarama"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

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

func newHealthServer(port string) *http.Server {
	healthMux := http.NewServeMux()
	healthMux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	return &http.Server{Addr: ":" + port, Handler: healthMux}
}

func runConsumerLoops(ctx context.Context, cg sarama.ConsumerGroup, topic string, handler sarama.ConsumerGroupHandler, logger *slog.Logger) {
	// Consumer error listener
	go func() {
		for err := range cg.Errors() {
			logger.Error("consumer group error", "error", err)
		}
	}()

	// Ingestion consumption loop
	go func() {
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
}

func initGRPCClient(target string) (*grpc.ClientConn, error) {
	return grpc.NewClient(
		target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
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
		defer shutdownTracer(context.Background())
	}

	// Establish gRPC connection to Processing Service with OpenTelemetry client stats handler
	grpcConn, err := initGRPCClient(cfg.ProcessingTarget)
	if err != nil {
		return fmt.Errorf("failed to create grpc client connection: %w", err)
	}
	defer grpcConn.Close()

	processingClient := eventpb.NewProcessingServiceClient(grpcConn)
	handler := NewIngestionHandler(processingClient, logger, cfg.GRPCTimeout)

	// Initialize Kafka Consumer Group
	saramaCfg := newSaramaConfig()
	consumerGroup, err := initConsumerGroup(cfg.KafkaBrokers, cfg.ConsumerGroupID, saramaCfg)
	if err != nil {
		return fmt.Errorf("failed to create sarama consumer group: %w", err)
	}
	defer consumerGroup.Close()

	// Lightweight HTTP health probe on port 8080 for container health checks
	healthPort := getEnv("HEALTH_PORT", "8080")
	httpHealthServer := newHealthServer(healthPort)
	go func() {
		if err := httpHealthServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("http health check server error", "error", err)
		}
	}()
	defer func() {
		_ = httpHealthServer.Shutdown(context.Background())
	}()

	runConsumerLoops(ctx, consumerGroup, cfg.KafkaTopic, handler, logger)

	logger.Info("ingestion service is actively consuming messages", "health_port", healthPort)
	<-ctx.Done()

	logger.Info("shutting down ingestion service gracefully")
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
