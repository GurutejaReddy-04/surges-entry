# Stage 1: Build
FROM golang:1.23-alpine AS builder

WORKDIR /app

ENV GOWORK=off

# Copy dependent proto module and service code
COPY proto/ ./proto/
COPY services/processing/ ./services/processing/

WORKDIR /app/services/processing
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /app/bin/processing .

# Stage 2: Runtime
FROM alpine:3.20

# Use Alpine rather than scratch to retain a lightweight debugging shell
RUN apk add --no-cache ca-certificates tzdata \
    && adduser -D -u 10001 -s /bin/sh appuser

WORKDIR /app
COPY --from=builder /app/bin/processing /app/processing

USER appuser

EXPOSE 50051 8080

HEALTHCHECK --interval=5s --timeout=3s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8080/healthz || exit 1

ENTRYPOINT ["/app/processing"]
