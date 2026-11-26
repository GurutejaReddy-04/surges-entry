# Stream Reliability & Fault-Tolerance

> *SurgesEntry Distributed Event Processing Platform*

---

## 1. The Core Reliability Contract

The SurgesEntry platform provides an **at-least-once delivery contract with idempotent audit persistence** for valid events, paired with a **drop-and-log policy for malformed or semantically invalid payloads**.

```text
                           Incoming Kafka Message
                                     |
                        +------------+------------+
                        v                         v
               Structurally Valid?        Malformed JSON?
                        |                         |
                       YES                        NO --> Drop & Log Warning
                        |                                (Preserves Partition Liveness)
                        v
               gRPC ProcessEvent()
                        |
         +--------------+--------------+
         v              v              v
     Success     codes.InvalidArgument  codes.Unavailable
         |              |              |
         v              v              v
   Mark Offset   Poison Pill Drop     Halt Partition Claim
  (session.Mark) (Preserves Liveness) (Do NOT Mark Offset;
                                       Exponential Backoff Rejoin)
```

### The Poison-Pill & Malformed Payload Boundary
The ingestion service drops invalid messages rather than retrying them indefinitely, which would stall the partition (see [`services/ingestion/handler.go`](../services/ingestion/handler.go)):

1. **Malformed JSON / Schema Failures**: If a message fails `ParseAndValidateEventWithFallback`, the consumer logs a structured warning and returns `nil` so the consumer advances past the corrupted payload.
2. **Downstream Validation Rejections (`codes.InvalidArgument`)**: If Processing rejects an event as invalid (e.g., empty `user_id`, empty `event_type`, `NaN`/`Inf`, or value outside `[-99999999.99, 99999999.99]`), retrying will never succeed. The handler logs a warning and returns `nil`.

---

## 2. Defensive Consumer Offset Management

1. **Manual Marking Only (`session.MarkMessage`)**: Offsets are marked only after `ProcessEvent` returns `success = true` (after durable PostgreSQL persistence).
2. **In-Place Exponential Backoff**: Transient gRPC errors trigger up to 3 in-place attempts (50ms, 100ms backoff between attempts) before failing the message.
3. **Partition Claim Halting on Hard Failures**: On persistent downstream failure (`codes.Unavailable`), `ProcessSingleMessage` returns an error, halting `ConsumeClaim()` **without calling `session.MarkMessage()`**.
4. **Consumer Loop Backoff**: `runConsumerLoops` applies bounded exponential backoff (250ms up to 10s) before rejoining the consumer group, flipping `/healthz` to HTTP `503` after 3 consecutive consume errors.

---

## 3. Consumer Outage Experiment

| Parameter | Specification |
|---|---|
| **Hypothesis** | Kafka durable partition log retains published events while Ingestion is down; restarted consumer resumes from uncommitted offset with zero loss. |
| **Test Environment** | Single-node Kafka KRaft (6 partitions), Redis 7, PostgreSQL 16, Ingestion, Processing, Notification. |
| **Failure Point** | Abrupt container/pod termination (`kubectl delete pod --now` or `docker stop`) of `surges-entry-ingestion`. |
| **Workload** | 10 baseline events -> Ingestion stopped -> 5 outage events -> Ingestion restarted -> PostgreSQL audited. |
| **Observed Result** | **15/15 events verified in PostgreSQL** (10 baseline + 5 outage buffered). MTTR < 6 seconds. |
| **Reproduction Script** | [`scripts/kill-replay-demo.ps1`](../scripts/kill-replay-demo.ps1) / [`scripts/kill-replay-demo.sh`](../scripts/kill-replay-demo.sh) |

### Scope & Assumptions
* Kafka broker and PostgreSQL remain healthy and retain committed state on disk.
* Single-broker local setup does not simulate multi-broker KRaft quorum loss or un-replicated disk failure (`RF=1`, `min.insync.replicas=1`).

---

## 4. Idempotency & Deterministic Stream Replay

SurgesEntry protects against duplicate side effects during crash redelivery and manual offset rewinds:

1. **Partition Key Affinity**: Producers key messages by `user_id` (`HashPartitioner`), keeping each user's events on a single partition in strict FIFO order.
2. **Deterministic Idempotency Key**: When an event omits `trace_id`, Ingestion derives a stable 32-hex key from `SHA-256(topic:partition:offset)[:16]`.
3. **PostgreSQL Deduplication**: `SaveProcessedEvent` uses `INSERT ... ON CONFLICT (trace_id) WHERE trace_id IS NOT NULL AND trace_id <> '' DO NOTHING RETURNING id`. Replayed events return `inserted = false`, skipping both Redis `CommitEvent` and duplicate alert dispatch.
4. **Clean Replay Verification**: When replaying from `--to-earliest` against a flushed Redis + truncated DB state, count-based sliding windows (`10` items) yield deterministic rolling averages independent of wall-clock drift. See [`scripts/offset-replay-demo.ps1`](../scripts/offset-replay-demo.ps1) / [`scripts/offset-replay-demo.sh`](../scripts/offset-replay-demo.sh).

---

## 5. Graceful Degradation (Two-Tier Anomaly Engine)

To isolate Redis outages from event ingestion:

* **Tier 1 (Dynamic Rolling Window)**: When Redis responds with a positive `rolling_avg`, Processing evaluates `value > 1.5 * rolling_avg` (configurable via `DEVIATION_MULTIPLIER`).
* **Tier 2 (Static Fallback Threshold)**: If Redis is unreachable, the window is empty, or `rolling_avg <= 0`, Processing logs a warning, persists `rolling_avg = NULL`, and evaluates `value > 1000.0` (configurable via `FALLBACK_THRESHOLD`).
