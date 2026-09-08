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

	eventpb "event-platform/proto/gen/event"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

func newNotificationHealthServer(port string, healthServer *health.Server) *http.Server {
	healthMux := http.NewServeMux()
	healthMux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		resp, err := healthServer.Check(r.Context(), &grpc_health_v1.HealthCheckRequest{
			Service: eventpb.NotificationService_ServiceDesc.ServiceName,
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

func setupNotificationGRPCServer(port string, server *NotificationServer, healthServer *health.Server) (*grpc.Server, net.Listener, error) {
	addr := fmt.Sprintf(":%s", port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to listen on tcp port %s: %w", port, err)
	}

	grpcServer := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)

	eventpb.RegisterNotificationServiceServer(grpcServer, server)

	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	healthServer.SetServingStatus(eventpb.NotificationService_ServiceDesc.ServiceName, grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)

	return grpcServer, listener, nil
}

func run(ctx context.Context, port, healthPort string, logger *slog.Logger) error {
	// Initialize OpenTelemetry TracerProvider
	shutdownTracer, err := InitTelemetry(ctx, "notification-service", logger)
	if err != nil {
		logger.Warn("failed to initialize tracer provider; running without telemetry", "error", err)
	} else {
		defer shutdownTracer(context.Background())
	}

	server := NewNotificationServer(logger)
	healthServer := health.NewServer()

	grpcServer, listener, err := setupNotificationGRPCServer(port, server, healthServer)
	if err != nil {
		return fmt.Errorf("failed to setup notification grpc server: %w", err)
	}

	httpHealthServer := newNotificationHealthServer(healthPort, healthServer)
	go func() {
		if err := httpHealthServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("http health check server error", "error", err)
		}
	}()

	serverErr := make(chan error, 1)
	go func() {
		logger.Info("notification service gRPC server started", "addr", listener.Addr().String(), "health_port", healthPort)
		if err := grpcServer.Serve(listener); err != nil && err != grpc.ErrServerStopped {
			serverErr <- err
		}
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutting down notification service gracefully")
		healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
		healthServer.SetServingStatus(eventpb.NotificationService_ServiceDesc.ServiceName, grpc_health_v1.HealthCheckResponse_NOT_SERVING)
		_ = httpHealthServer.Shutdown(context.Background())
		grpcServer.GracefulStop()
		logger.Info("notification service stopped")
		return nil
	case err := <-serverErr:
		return fmt.Errorf("notification server runtime error: %w", err)
	}
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	port := getEnv("GRPC_PORT", "50052")
	healthPort := getEnv("HEALTH_PORT", "8080")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, port, healthPort, logger); err != nil {
		logger.Error("notification server runtime error", "error", err)
		os.Exit(1)
	}
}
