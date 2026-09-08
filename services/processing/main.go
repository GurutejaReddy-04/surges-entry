package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	eventpb "event-platform/proto/gen/event"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func newProcessingHealthServer(port string, healthServer *health.Server) *http.Server {
	healthMux := http.NewServeMux()
	healthMux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		resp, err := healthServer.Check(r.Context(), &grpc_health_v1.HealthCheckRequest{
			Service: eventpb.ProcessingService_ServiceDesc.ServiceName,
		})
		if err != nil || resp.Status != grpc_health_v1.HealthCheckResponse_SERVING {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("unhealthy"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	return &http.Server{Addr: ":" + port, Handler: healthMux}
}

func initNotificationClient(addr string) (*grpc.ClientConn, eventpb.NotificationServiceClient, error) {
	notifConn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		return nil, nil, err
	}
	return notifConn, eventpb.NewNotificationServiceClient(notifConn), nil
}

func setupProcessingGRPCServer(port string, processor *EventProcessor, healthServer *health.Server) (*grpc.Server, net.Listener, error) {
	addr := fmt.Sprintf(":%s", port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to listen on tcp port %s: %w", port, err)
	}

	grpcServer := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)

	eventpb.RegisterProcessingServiceServer(grpcServer, processor)

	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	healthServer.SetServingStatus(eventpb.ProcessingService_ServiceDesc.ServiceName, grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)

	return grpcServer, listener, nil
}

func run(ctx context.Context, cfg Config, logger *slog.Logger) error {
	logger.Info("starting processing service",
		"grpc_port", cfg.GRPCPort,
		"redis_addr", cfg.RedisAddr,
		"postgres_dsn_configured", cfg.PostgresDSN != "",
		"notification_addr", cfg.NotificationAddr,
		"deviation_multiplier", cfg.DeviationMultiplier,
		"fallback_threshold", cfg.FallbackThreshold,
		"redis_window_size", cfg.RedisWindowSize,
	)

	// Initialize OpenTelemetry TracerProvider
	shutdownTracer, err := InitTelemetry(ctx, "processing-service", logger)
	if err != nil {
		logger.Warn("failed to initialize tracer provider; running without telemetry", "error", err)
	} else {
		defer shutdownTracer(context.Background())
	}

	// Initialize Redis hot state store
	hotState := NewRedisHotStateStore(cfg.RedisAddr, cfg.RedisWindowSize, logger)
	defer hotState.Close()

	// Initialize PostgreSQL repository
	initCtx, initCancel := context.WithTimeout(ctx, 5*time.Second)
	defer initCancel()
	repo, err := NewPostgresEventRepository(initCtx, cfg.PostgresDSN, logger)
	if err != nil {
		return fmt.Errorf("failed to connect to postgresql database: %w", err)
	}
	defer repo.Close()

	// Initialize Notification gRPC client
	notifConn, notifClient, err := initNotificationClient(cfg.NotificationAddr)
	if err != nil {
		logger.Warn("failed to initialize notification client; alerts will fail until service is reachable", "error", err)
	} else {
		defer notifConn.Close()
	}

	// Initialize gRPC processor and server
	processor := NewEventProcessor(hotState, repo, notifClient, cfg, logger)
	defer processor.Stop()

	healthServer := health.NewServer()
	grpcServer, listener, err := setupProcessingGRPCServer(cfg.GRPCPort, processor, healthServer)
	if err != nil {
		return fmt.Errorf("failed to setup grpc server: %w", err)
	}

	// Lightweight HTTP health probe on port 8080 (delegates to grpc.health.v1) for container health checks
	healthPort := getEnv("HEALTH_PORT", "8080")
	httpHealthServer := newProcessingHealthServer(healthPort, healthServer)
	go func() {
		if err := httpHealthServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("http health server error", "error", err)
		}
	}()

	serverErr := make(chan error, 1)
	go func() {
		logger.Info("processing service gRPC server is listening", "addr", listener.Addr().String(), "health_port", healthPort)
		if err := grpcServer.Serve(listener); err != nil && err != grpc.ErrServerStopped {
			serverErr <- err
		}
	}()

	select {
	case <-ctx.Done():
		logger.Info("gracefully terminating processing service")
		healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
		healthServer.SetServingStatus(eventpb.ProcessingService_ServiceDesc.ServiceName, grpc_health_v1.HealthCheckResponse_NOT_SERVING)
		_ = httpHealthServer.Shutdown(context.Background())
		grpcServer.GracefulStop()
		logger.Info("processing service shutdown complete")
		return nil
	case err := <-serverErr:
		return fmt.Errorf("grpc server encountered fatal error: %w", err)
	}
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	cfg := LoadConfig()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg, logger); err != nil {
		logger.Error("server encountered fatal runtime error", "error", err)
		os.Exit(1)
	}
}
