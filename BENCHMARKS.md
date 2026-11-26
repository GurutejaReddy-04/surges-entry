# SurgesEntry — Benchmark Methodology & Performance Baselines

> *Measurement conditions, simulator workload parameters, and reproduction steps for the distributed event processing pipeline.*

---

## 1. Baseline Measurements Summary

| Metric | Measured Baseline | Measurement Scope | Notes |
|---|---|---|---|
| **Pipeline Throughput** | 100 - 250 events/sec | Sustained rate under simulator | Requires `EVENT_RATE=200` override |
| **End-to-End Latency (p50)** | **2.1 ms** | Ingestion -> gRPC -> Redis -> Postgres | Sampled across 5,000 traces |
| **End-to-End Latency (p95)** | **4.8 ms** | Includes anomaly detection & async alert | Sampled across 5,000 traces |
| **End-to-End Latency (p99)** | **8.2 ms** | Tail latency under burst load | Sampled across 5,000 traces |
| **Failover Recovery (MTTR)** | **< 6.0 seconds** | Consumer kill to first DB insert | Verified via `kill-replay-demo` |
| **Total Continuous Events** | **75,000+ events** | Cumulative verification run | 3 runs x 25,000 events |
| **Container Memory Footprint** | **~18 MB RSS** | Steady-state Go runtime RSS per service | Measured via `docker stats` |

> [!NOTE]
> The figures above reflect single-node developer workstation benchmarks using local Docker containers.

---

## 2. Test Environment & Configuration

### Hardware & Operating Environment
* **Host CPU**: 8 physical cores / 16 threads, x86_64 architecture (3.2 GHz base clock).
* **RAM**: 16 GB system memory.
* **Storage**: NVMe PCIe M.2 SSD.
* **Container Runtime**: Docker Desktop on Windows 11 with WSL2 backend (4 vCPUs and 8 GB RAM allocated).

### Infrastructure Configuration
* **Apache Kafka (KRaft mode)**: Confluent Platform `cp-kafka:7.7.0`, 1 broker, 6 partitions, `acks=all`, `min.insync.replicas=1`, `auto.create.topics=true`.
* **Redis**: `redis:7-alpine`, single instance, TCP port 6379.
* **PostgreSQL**: `postgres:16-alpine`, `pgxpool` configured in Go (`MaxConns=15`, `MinConns=2`, 3-second ping timeout).
* **Microservices**: Go 1.23 (`CGO_ENABLED=0`, Alpine 3.20 base image), single replica per service.

---

## 3. Simulator Workload Parameters

In [`simulator/main.go`](simulator/main.go), the traffic generator defaults to low-rate interactive traffic (`EVENT_RATE=5`, `NUM_USERS=10`):

```go
func loadConfig() Config {
    broker := envOr("KAFKA_BROKER", envOr("KAFKA_BROKERS", "localhost:9092"))
    return Config{
        KafkaBroker: broker,
        EventRate:   envIntOr("EVENT_RATE", 5),   // Default: 5 events/sec
        NumUsers:    envIntOr("NUM_USERS", 10),   // Default: 10 synthetic users
    }
}
```

> [!IMPORTANT]
> Running `go run ./simulator` without overrides emits **5 events/sec**.
> To reproduce the **100-250 events/sec** benchmark workload, set `EVENT_RATE=200` and `NUM_USERS=50`.

### Benchmark Workload Profile
* **Event Rate**: `EVENT_RATE=200` (200 events/second target).
* **User Pool**: `NUM_USERS=50` (distributes keys evenly across all 6 Kafka partitions via `HashPartitioner`).
* **Value Distribution**: Normal distribution (`mean=100`, `stddev=20`) with ~5% synthetic outliers (`200.0 <= value <= 500.0`).
* **Event Types**: Uniform selection across `transaction`, `login`, `page_view`, `purchase`, and `api_call`.

---

## 4. Measurement Methodology

1. **Distributed Trace Latency**:
   - OpenTelemetry spans propagate across gRPC calls via W3C `traceparent` headers.
   - Root span: `ingestion.consume_event` (Ingestion).
   - Downstream RPC: `event.ProcessingService/ProcessEvent` (Processing).
   - Percentiles (p50, p95, p99) are computed from Jaeger span durations over 5,000 recorded traces.
2. **Warm-Up Procedure**:
   - Execute a 60-second warm-up at 50 events/sec to stabilize Go GC and warm Kafka, Redis, and PostgreSQL connection pools.
3. **Cumulative Volume**:
   - Measured across 3 consecutive runs of 25,000 events each without service restarts.
4. **MTTR (Mean Time to Recovery)**:
   - Measured by [`scripts/kill-replay-demo.ps1`](scripts/kill-replay-demo.ps1) as the elapsed time from terminating the Ingestion pod/container to the first post-recovery row inserted into PostgreSQL.

---

## 5. Reproduction Steps

### Step 1: Start Infrastructure & Services
```bash
cp .env.example .env
docker compose up -d
docker compose -f docker-compose.local.yml up -d --build
```

### Step 2: Warm Up the Pipeline (60 seconds)
```bash
EVENT_RATE=50 NUM_USERS=20 go run ./simulator
```

### Step 3: Run the Benchmark Workload (200 events/sec)
```bash
# Linux / macOS
EVENT_RATE=200 NUM_USERS=50 go run ./simulator

# Windows (PowerShell)
$env:EVENT_RATE="200"; $env:NUM_USERS="50"; go run ./simulator
```

### Step 4: Inspect Jaeger Traces
```bash
curl -s "http://localhost:16686/api/traces?service=processing-service&limit=1000" > traces.json
```

### Step 5: Verify Database Count & Ingestion Window
```bash
docker exec -it postgres psql -U eventplatform -d eventplatform -c \
  "SELECT count(*), min(ingested_at), max(ingested_at) FROM processed_events;"
```
