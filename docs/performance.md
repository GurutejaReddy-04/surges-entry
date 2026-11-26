# Performance Engineering & Latency Analysis

> *SurgesEntry Distributed Event Processing Platform*

---

## 1. Performance Overview & Baselines

SurgesEntry is engineered for low-latency streaming ingestion and real-time statistical evaluation.

```text
Kafka Consumer --> Ingestion (:8080) --> gRPC (:50051) --> Processing Service
                                                                  |
                 +------------------------------------------------+--------------------+
                 v                                                v                    v
        Redis Hot State                                   PostgreSQL Audit     Async Alert Queue
 (ComputeAverage -> DB -> CommitEvent)               (INSERT ON CONFLICT)    (Buffered Worker Pool)
```

### Baseline Performance Summary

| Metric | Measured Baseline | Target SLA | System Impact |
|---|---|---|---|
| **Pipeline Throughput** | 100 - 250 events/sec | > 100 events/sec | Single consumer replica; scalable via Kafka partitions |
| **End-to-End Latency (p50)** | **2.1 ms** | < 5.0 ms | Ingestion + gRPC + Redis + Postgres insert |
| **End-to-End Latency (p95)** | **4.8 ms** | < 10.0 ms | Includes anomaly detection and alert evaluation |
| **End-to-End Latency (p99)** | **8.2 ms** | < 15.0 ms | Tail latency under burst simulator load |
| **Failover Recovery (MTTR)** | **< 6.0 seconds** | < 10.0 seconds | Consumer pod crash to stream resumption |
| **Container Memory (RSS)** | **~18 MB RSS** | < 64 MB | Minimal Go runtime memory footprint |

*(For reproduction instructions, hardware specifications, and simulator overrides, see [BENCHMARKS.md](../BENCHMARKS.md).)*

---

## 2. Two-Phase Sliding-Window Hot State (Redis)

Calculating per-user rolling averages at scale requires low overhead, strict concurrency safety, and resistance to outlier self-pollution and dual-write desync. SurgesEntry implements this in [`services/processing/hotstate.go`](../services/processing/hotstate.go) and [`services/processing/server.go`](../services/processing/server.go).

### 2.1 The Outlier Self-Pollution & Dual-Write Problems
- **Self-pollution**: Pushing an incoming spike to the user's window before computing the baseline elevates the average and masks subsequent anomalies.
- **Dual-write desync**: Writing to Redis before PostgreSQL persistence succeeds would corrupt the rolling window if PostgreSQL fails and Kafka redelivers the event.

### 2.2 Per-User Serialized Two-Phase Pipeline
`EventProcessor.ProcessEvent` acquires a striped per-user lock (`256` lock stripes hashed by `user_id`) and executes two separate Redis `TxPipeline` calls around the PostgreSQL write:

1. **Pre-DB Baseline Read (`ComputeAverage`)**:
   ```go
   pipe := s.client.TxPipeline()
   lrangeCmd := pipe.LRange(ctx, key, 0, int64(winSize-1))
   pipe.Expire(ctx, key, 3600*time.Second)
   _, err := pipe.Exec(ctx)
   ```
2. **PostgreSQL Idempotent Insert**:
   Persists the record with `ON CONFLICT (trace_id) ... DO NOTHING RETURNING id`.
3. **Post-DB Window Commit (`CommitEvent`, only when `inserted == true`)**:
   - **Normal events**: `LPUSH` -> `LTRIM 0 (winSize-1)` -> `EXPIRE 3600s` in a `TxPipeline`.
   - **Anomalies**: Renews TTL only (`EXPIRE 3600s`) without appending the outlier to the window.
   - **Duplicates (`inserted == false`)**: Skips Redis commit entirely.

---

## 3. Asynchronous Bounded Notification Dispatch

When an anomaly is flagged for a newly inserted event, forwarding an alert to the Notification Service does not block the primary ingestion path.

SurgesEntry uses an asynchronous bounded worker pool in [`services/processing/server.go`](../services/processing/server.go):

```go
type EventProcessor struct {
    alertQueue chan *alertTask // Bounded buffer: 1000 items
    // ...
}
```

* **Non-Blocking Enqueue**: Processing enqueues alerts via a non-blocking `select`:
  ```go
  select {
  case s.alertQueue <- task:
  default:
      s.logger.WarnContext(ctx, "alert queue full, dropping notification dispatch", ...)
  }
  ```
* **Dedicated Worker Goroutines**: 4 worker goroutines drain `alertQueue` and issue `SendAlert` RPCs, with bounded queue draining on graceful shutdown (`Stop()`).
* **Latency Isolation**: Slowdowns or restarts in Notification Service do not stall Kafka partition consumption.

---

## 4. Distributed Tracing & Span Structure

Each ingested event generates a 5-span distributed trace across the three services:

```text
[ingestion.consume_event] (Root span in Ingestion)
  +-- [client: event.ProcessingService/ProcessEvent] (Ingestion gRPC client)
        +-- [server: event.ProcessingService/ProcessEvent] (Processing gRPC server)
              +-- (async) [client: event.NotificationService/SendAlert] (Processing gRPC client, anomalies only)
                    +-- [server: event.NotificationService/SendAlert] (Notification gRPC server)
```

* **Context Propagation**: W3C `traceparent` metadata is injected and extracted via `otelgrpc.NewClientHandler()` and `otelgrpc.NewServerHandler()`.
* **Correlation Key**: The root `ingestion.consume_event` span records `event.idempotency_key` matching the deterministic `trace_id` column stored in PostgreSQL `processed_events`, enabling exact correlation between Jaeger traces and audit rows without breaking replay deduplication.
