# Stream Reliability & Fault-Tolerance

> *SurgesEntry Distributed Event Processing Platform*

---

## 1. The Core Reliability Contract

The SurgesEntry platform provides an **at-least-once processing contract for structurally and semantically valid events**, coupled with a **deliberate discard policy (drop-and-log) for malformed or invalid events**.

```
                           Incoming Kafka Message
                                     │
                        ┌────────────┴────────────┐
                        ▼                         ▼
               Structurally Valid?        Malformed JSON?
                        │                         │
                       YES                        NO ──► Drop & Log Warning
                        │                                (Preserves Partition Liveness)
                        ▼
               gRPC ProcessEvent()
                        │
         ┌──────────────┼──────────────┐
         ▼              ▼              ▼
     Success     codes.InvalidArgument  codes.Unavailable
         │              │              │
         ▼              ▼              ▼
   Mark Offset   Poison Pill Drop     Halt Partition Claim
  (session.Mark) (Preserves Liveness) (Do NOT Mark Offset;
                                       Retry on Restart)
```

### The Poison-Pill & Malformed Payload Boundary
In distributed stream processing, claiming universal "zero data loss" across all input conditions is technically ungrounded. In SurgesEntry, the ingestion layer enforces an explicit architectural boundary in [`services/ingestion/handler.go`](../services/ingestion/handler.go):

1. **Malformed JSON Payloads**: If a message cannot be parsed or validated against the protobuf schema (`ParseAndValidateEvent`), the consumer logs a structured warning and returns `nil`. This deliberately discards the malformed payload rather than stalling the partition consumer loop indefinitely.
2. **Downstream Validation Rejections (`codes.InvalidArgument`)**: If downstream processing rejects an event as semantically unprocessable, retrying will never succeed. The handler treats this as a poison pill, logs an alert, and returns `nil` (`// Drop and continue`).

**Design Rationale**: A single unparseable message must never cause head-of-line blocking for thousands of valid user events queued on the same partition. SurgesEntry intentionally prioritizes partition throughput and system availability over retaining garbage bytes.

---

## 2. Defensive Consumer Offset Management

Kafka consumer group offsets monotonically advance if not managed defensively. Default auto-commit implementations can mark failed offsets as consumed, causing silent data loss.

SurgesEntry eliminates this failure mode through explicit, transactional offset mechanics:

1. **Manual Marking Only (`session.MarkMessage`)**: Offsets are committed only after the downstream gRPC RPC succeeds and the event is durably persisted to PostgreSQL.
2. **In-Place Exponential Backoff**: When encountering transient gRPC errors, the consumer executes up to 3 retries in-place (50ms, 100ms, 200ms backoff).
3. **Partition Claim Halting on Hard Failures**: If downstream services return `codes.Unavailable` (e.g., PostgreSQL or Processing is unreachable after all retries), `ProcessSingleMessage` returns an error, halting the `ConsumeClaim()` loop immediately **without calling `session.MarkMessage()`**.
4. **Resumption from Last Committed Offset**: When the pod restarts or Kafka rebalances, consumption resumes precisely from the uncommitted failed offset, guaranteeing at-least-once delivery.

---

## 3. Empirical Verification: Consumer Outage Experiment

Rather than asserting unverified universal guarantees, SurgesEntry documents the precise experimental protocol used to verify crash recovery.

### Experiment Summary

| Parameter | Specification |
|---|---|
| **Hypothesis** | Kafka durable partition buffer retains in-flight published events during abrupt consumer termination; pod self-healing recovers 100% of buffered events without loss. |
| **Test Environment** | Local single-node Kafka KRaft (6 partitions), PostgreSQL 16 store, Ingestion & Processing microservices. |
| **Failure Point** | Abrupt SIGKILL / pod deletion (`kubectl delete pod --now` or `docker stop`) of `surges-entry-ingestion`. |
| **Workload** | 10 baseline events published -> Ingestion killed mid-stream -> 5 events published during outage -> Ingestion restarted -> DB records audited. |
| **Observed Result** | **15/15 events verified in PostgreSQL** (10 baseline + 5 recovered from outage buffer). MTTR < 6 seconds. |
| **Reproduction Script** | [`scripts/kill-replay-demo.ps1`](../scripts/kill-replay-demo.ps1) / [`scripts/kill-replay-demo.sh`](../scripts/kill-replay-demo.sh) |

### Explicit Assumptions
This experimental verification holds under the following conditions:
* The Kafka broker remains operational and retains topic durability (filesystem fsync).
* The PostgreSQL audit database remains accessible upon consumer recovery.
* Consumer group offset coordination topics (`__consumer_offsets`) remain healthy.

### Known Limitations & Scope
* **Single Broker Topology**: The local experiment does not simulate broker split-brain, multi-broker KRaft quorum failure, or broker disk destruction without active replication (`min.insync.replicas > 1`).
* **At-Least-Once Duplication**: If a consumer process is terminated in the narrow window between writing to PostgreSQL and committing the Kafka offset, the event may be reprocessed upon restart.

---

## 4. Deterministic Stream Replayability

SurgesEntry supports deterministic stream reprocessing on demand via Kafka offset rewinds:

1. **Partition Key Affinity**: Kafka hashes message keys by `user_id`. All events for a specific user land on the same partition in strict FIFO sequence.
2. **Count-Based vs. Time-Based Windows**: Time-based tumbling/sliding windows suffer from clock drift, network delays, and watermark skew during batch replay. SurgesEntry uses count-based sliding windows (last 10 events per user).
3. **Replay Determinism**: Because user events are strictly ordered and window evaluation is count-based, rewinding the consumer group offset (`--to-earliest`) produces the exact same statistical moving averages as real-time processing.
4. **Verification Script**: [`scripts/offset-replay-demo.ps1`](../scripts/offset-replay-demo.ps1) / [`scripts/offset-replay-demo.sh`](../scripts/offset-replay-demo.sh).

---

## 5. Graceful Degradation (Two-Tier Anomaly Engine)

To isolate cache infrastructure failures from the ingestion stream, the Processing Service implements a two-tier anomaly detection hierarchy:

* **Tier 1 (Dynamic Rolling Window)**: When Redis is reachable, evaluates event value against $1.5 \times \text{rolling\_avg}$ using atomic `rdb.TxPipeline()` pre-write evaluation.
* **Tier 2 (Static Fallback Heuristic)**: If Redis connection times out or fails, the call is caught within an isolated error boundary. Processing logs a warning, sets `rolling_avg = NULL`, and evaluates against a static threshold (`value > 1000.0`). The event is persisted to PostgreSQL and the gRPC call returns success. Cache failure never halts stream ingestion.
