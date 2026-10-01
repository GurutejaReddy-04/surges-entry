# Performance Engineering & Latency Analysis

> *SurgesEntry Distributed Event Processing Platform*

---

## 1. Performance Overview & Historical Baselines

SurgesEntry is engineered for low-latency streaming ingestion and real-time statistical inference.

```
       Kafka Consumer ──► Ingestion (:8080) ──► gRPC (:50051) ──► Processing Service
                                                                       │
                      ┌────────────────────────────────────────────────┼───────────────────────┐
                      ▼                                                ▼                       ▼
            Atomic Redis Pipeline                              PostgreSQL Audit         Async Alert Queue
           (LRANGE + LPUSH + LTRIM)                        (INSERT via pgxpool)       (Buffered Worker Pool)
               [Single RTT: ~0.8ms]                            [Indexed: ~1.2ms]          [Non-blocking: ~0.1ms]
```

### Baseline Performance Summary

| Metric | Measured Baseline | Target SLA | System Impact |
|---|---|---|---|
| **Pipeline Throughput** | 100 – 250 events/sec | > 100 events/sec | Single consumer replica; scalable via Kafka partitions |
| **End-to-End Latency (p50)** | **2.1 ms** | < 5.0 ms | Ingestion + gRPC + Redis pipelining + Postgres insert |
| **End-to-End Latency (p95)** | **4.8 ms** | < 10.0 ms | Includes anomaly detection and alerting evaluation |
| **End-to-End Latency (p99)** | **8.2 ms** | < 15.0 ms | Tail latency under burst simulator load |
| **Failover Recovery (MTTR)** | **< 6.0 seconds** | < 10.0 seconds | Consumer pod crash to stream resumption |
| **Container Memory (RSS)** | **~18 MB RSS** | < 64 MB | Minimal Go runtime memory footprint |

*(For reproduction instructions, hardware specifications, and simulator overrides, see [BENCHMARKS.md](../BENCHMARKS.md).)*

---

## 2. Atomic Single-Round-Trip Hot State (Redis)

Calculating per-user rolling averages at scale requires low network overhead and strong concurrency isolation. SurgesEntry implements this in [`services/processing/hotstate.go`](../services/processing/hotstate.go) using Redis transaction pipelining (`MULTI/EXEC`).

### 2.1 The Outlier Self-Pollution Problem
If a naive implementation pushes the incoming value to the user's history list *before* calculating the average, a massive anomalous spike (e.g., $10\times$ normal) pollutes its own baseline. This artifically elevates the rolling average, potentially masking subsequent anomalies.

### 2.2 The Atomic Pipeline Solution
SurgesEntry queues `LRANGE` **before** `LPUSH` within a single Redis atomic transaction:

```go
pipe := s.client.TxPipeline()
lrangeCmd := pipe.LRange(ctx, key, 0, int64(s.windowSize-1)) // 1. Read historical baseline
pipe.LPush(ctx, key, value)                                   // 2. Append new event
pipe.LTrim(ctx, key, 0, int64(s.windowSize-1))               // 3. Cap window to 10 items
pipe.Expire(ctx, key, time.Hour)                             // 4. Reset TTL
_, err := pipe.Exec(ctx)
```

**Benefits**:
1. **Uncontaminated Baseline**: The statistical baseline is computed strictly over the pre-existing history prior to the new event arriving.
2. **Single Network Round-Trip**: Combines 4 discrete commands into a single round-trip, reducing cache operation latency from $\approx 3.2\text{ ms}$ to $\approx 0.8\text{ ms}$.
3. **Atomic Multi-Replica Safety**: Redis single-threaded execution guarantees that concurrent replicas cannot interleave commands for the same user key.

---

## 3. Asynchronous Bounded Notification Dispatch

When an anomaly is flagged, forwarding an alert to the Notification Service must not add latency or backpressure to the primary ingestion stream.

SurgesEntry uses an asynchronous bounded worker pool in [`services/processing/server.go`](../services/processing/server.go):

```go
type EventProcessor struct {
    alertQueue chan *eventpb.AlertRequest // Bounded buffer: 1000 items
    // ...
}
```

* **Non-Blocking Enqueue**: Processing pushes alerts to `alertQueue` using a non-blocking `select`:
  ```go
  select {
  case p.alertQueue <- alert:
  default:
      p.logger.Warn("alert queue full; dropping alert to protect throughput")
  }
  ```
* **Dedicated Worker Goroutines**: 2 worker goroutines drain `alertQueue` and issue gRPC `SendAlert` RPCs.
* **Latency Isolation**: If the Notification Service slows down or restarts, primary event processing latency remains unaffected.

---

## 4. Distributed Tracing & Flame Graph Analysis

Every ingested event is instrumented with OpenTelemetry and correlated across 5 distributed spans:

```
[ingestion.consume_event] (Root: ~2.1ms)
  └── [client: event.ProcessingService/ProcessEvent] (~1.9ms)
        └── [server: event.ProcessingService/ProcessEvent] (~1.8ms)
              ├── [redis.TxPipeline] (~0.8ms)
              ├── [postgres.insert_event] (~1.2ms)
              └── (async) [client: event.NotificationService/SendAlert] (~0.4ms)
                    └── [server: event.NotificationService/SendAlert] (~0.2ms)
```

* **Context Propagation**: W3C `traceparent` metadata is automatically injected and extracted via `otelgrpc.NewClientHandler()` and `otelgrpc.NewServerHandler()`.
* **Trace Storage**: Every database audit row in PostgreSQL records the hex-encoded 32-character `trace_id`, enabling instant lookup in Jaeger for any flagged audit event.
