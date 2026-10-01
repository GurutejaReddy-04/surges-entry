# SurgesEntry — Distributed Event Processing Platform
> *Distributed event processing platform in Go with Kafka, gRPC, Redis, and Kubernetes.*

[![CI](https://github.com/GurutejaReddy-04/surges-entry/actions/workflows/ci.yml/badge.svg)](https://github.com/GurutejaReddy-04/surges-entry/actions/workflows/ci.yml)
[![GitHub Repository](https://img.shields.io/badge/GitHub-surges--entry-blue?style=flat&logo=github)](https://github.com/GurutejaReddy-04/surges-entry)
[![Go Version](https://img.shields.io/badge/Go-1.22%2B-00ADD8?style=flat&logo=go)](https://go.dev/)
[![Kafka](https://img.shields.io/badge/Kafka-KRaft%207.7-231F20?style=flat&logo=apachekafka)](https://kafka.apache.org/)
[![gRPC](https://img.shields.io/badge/gRPC-Protobuf-244c5a?style=flat&logo=grpc)](https://grpc.io/)
[![OpenTelemetry](https://img.shields.io/badge/OpenTelemetry-v1.28-425CC7?style=flat&logo=opentelemetry)](https://opentelemetry.io/)
[![Kubernetes](https://img.shields.io/badge/Kubernetes-1.28%2B-326CE5?style=flat&logo=kubernetes)](https://kubernetes.io/)
[![Docker](https://img.shields.io/badge/Docker-Images%20%3C41MB-2496ED?style=flat&logo=docker)](https://www.docker.com/)

**SurgesEntry** is an event processing service written in Go. It reads events from Kafka, calculates rolling averages using Redis, flags anomalies, saves records to PostgreSQL, and uses OpenTelemetry for tracing.

---

## 1. System Architecture

```mermaid
flowchart TD
    subgraph Host ["Traffic Generation"]
        SIM["Event Simulator<br/>(Synthetic stream: N(100, 20), ~5% Outliers)"]
    end

    subgraph KafkaCluster ["Message Broker Tier"]
        KAFKA["Kafka KRaft Cluster (6 Partitions)<br/>Topic: 'events' | Partition Key: Hash(user_id)"]
    end

    subgraph Microservices ["Microservice Mesh (gRPC)"]
        ING["Ingestion Service<br/>(Sarama Consumer Group)<br/>Span: ingestion.consume_event"]
        PROC["Processing Service<br/>(Two-Tier Anomaly Engine)<br/>Span: ProcessEvent"]
        NOTIF["Notification Service<br/>(Alert Logging Engine)<br/>Span: SendAlert"]
    end

    subgraph StateTier ["Stateful Storage & Caching"]
        REDIS[("Redis 7 (Hot State)<br/>Atomic TxPipeline (MULTI/EXEC)<br/>Count-based Window (10 items)")]
        POSTGRES[("PostgreSQL 16 (Audit Store)<br/>Table: processed_events<br/>Indexed by trace_id & user_id")]
    end

    subgraph Observability ["Telemetry"]
        JAEGER["Jaeger Collector (OTLP gRPC :4317)<br/>Distributed Trace Visualizer"]
    end

    SIM -->|"Keyed Produce"| KAFKA
    KAFKA -->|"Partition Stream"| ING
    ING -->|"gRPC: ProcessEvent (traceparent)"| PROC
    PROC -->|"Atomic TxPipeline (single RTT)"| REDIS
    PROC -->|"Insert Audit Record"| POSTGRES
    PROC -->|"Async Bounded Dispatch"| NOTIF

    ING -.->|"OTLP"| JAEGER
    PROC -.->|"OTLP"| JAEGER
    NOTIF -.->|"OTLP"| JAEGER
```

---

## 2. Key Engineering Highlights

* **Partition Key Affinity**: Messages are keyed by `Hash(user_id)`, guaranteeing that all events for a specific user land on the same partition in strict FIFO order. This eliminates distributed locking across processing workers.
* **Redis Transactions**: Uses Redis `TxPipeline` to avoid race conditions.
* **Fallback Thresholds**: Uses a static threshold if Redis is down.
* **At-Least-Once Delivery & Poison Pill Discard**: Offsets are committed only after successful PostgreSQL persistence. Downstream outages halt the partition claim without monotonic advancement. Unparseable JSON and semantic invalid arguments (`codes.InvalidArgument`) are deliberately dropped and logged (`return nil`) to preserve partition liveness.
* **Distributed Observability**: Instrumentated with OpenTelemetry Go SDK and `otelgrpc`, generating 5-span flame graphs in Jaeger correlated by W3C `traceparent` headers and stored in PostgreSQL.

---

## 3. Performance & Reliability Summary

| Metric | Measured Baseline Value | Context & Provenance |
|---|---|---|
| **Pipeline Throughput** | **100 – 250 events/sec** | Single consumer replica; requires `EVENT_RATE=200` override |
| **End-to-End Latency (p50)** | **2.1 ms** | Ingestion -> gRPC -> Redis TxPipeline -> Postgres insert |
| **End-to-End Latency (p95)** | **4.8 ms** | Includes anomaly detection and alert evaluation |
| **End-to-End Latency (p99)** | **8.2 ms** | Tail latency under burst simulator load |
| **Failover Recovery (MTTR)** | **< 6.0 seconds** | Empirically verified consumer process crash-recovery |
| **Container Memory (RSS)** | **~18 MB RSS** | Steady-state Go runtime memory footprint |
| **Crash Resilience Proof** | **15/15 events recovered** | 10 baseline + 5 outage buffered; verified in PostgreSQL |

> [!NOTE]
> For detailed benchmark hardware specifications, simulator parameters, and reproduction steps, see [BENCHMARKS.md](BENCHMARKS.md). For reliability experiment conditions, assumptions, and limitations, see [docs/reliability.md](docs/reliability.md).

---

## 4. Quick Start Guide

### Prerequisites
* **Docker** & **Docker Compose**
* **Go 1.22+**
* **PowerShell 7+** (Windows) or **Bash** (Linux/macOS)

### 1. Initialize Environment & Start Stack
```bash
# Clone the repository
git clone https://github.com/GurutejaReddy-04/surges-entry.git
cd surges-entry

# Configure local environment from template (required for Compose)
cp .env.example .env    # On Windows: Copy-Item .env.example .env

# Start stateful infrastructure (Kafka, Redis, Postgres, Jaeger)
docker compose up -d

# Build and start microservices (Ingestion, Processing, Notification)
docker compose -f docker-compose.local.yml up -d --build
```

### 2. Verify Stack Health
```powershell
# Run the automated health & OpenTelemetry verification script
./verify.ps1
pwsh ./scripts/verify-phase8.ps1
```

### 3. Run the Traffic Simulator
```bash
# Interactive run (default 5 events/sec)
go run ./simulator

# High-rate benchmark run (200 events/sec across 50 users)
EVENT_RATE=200 NUM_USERS=50 go run ./simulator
```

---

## 5. Kubernetes Deployment

The repository includes declarative Kubernetes manifests configured for local demonstration (Docker Desktop / Minikube):

```bash
cd k8s

# 1. Configure local secrets from template
cp secret.example.yaml secret.yaml   # On Windows: Copy-Item secret.example.yaml secret.yaml

# 2. Deploy manifests with preflight validation
./deploy.sh   # On Windows: ./deploy.ps1
```

> [!NOTE]
> The local deployment routes dependencies using `host.docker.internal` to avoid heavy in-cluster storage engines on developer machines. For the full multi-AZ cloud production reference topology (AWS MSK, ElastiCache, RDS Aurora, Vault/ESO), see [docs/architecture.md](docs/architecture.md).

---

## 6. Technical Documentation Index

Deep-dive documentation is modularized in the [`docs/`](docs/) directory:

| Guide | Description |
|---|---|
| **[docs/architecture.md](docs/architecture.md)** | Full system data flow, local hybrid vs. reference cloud production topology, and architectural trade-offs. |
| **[docs/reliability.md](docs/reliability.md)** | At-least-once delivery contract, poison pill drop-and-log boundary, crash-recovery experiment, and offset replay. |
| **[docs/performance.md](docs/performance.md)** | Redis `TxPipeline` atomicity, outlier self-pollution defense, async alerting, and trace latency breakdown. |
| **[BENCHMARKS.md](BENCHMARKS.md)** | Benchmark provenance, simulator configuration gap (`EVENT_RATE=200`), hardware specs, and reproduction protocol. |
| **[docs/security.md](docs/security.md)** | Secret isolation (`.env.example`, `secret.example.yaml`), non-root container sandboxing, and historical credential audit. |
| **[k8s/README.md](k8s/README.md)** | Kubernetes manifests, security contexts, probes, manual scaling, and secret setup. |
| **[docs/demo/RECORDING_GUIDE.md](docs/demo/RECORDING_GUIDE.md)** | Terminal recording walkthrough for chaos failover and historical stream replay. |
| **[proto/event.proto](proto/event.proto)** | Protocol Buffer service contracts and event schemas. |

---

## License

This project is licensed under the [MIT License](LICENSE).
