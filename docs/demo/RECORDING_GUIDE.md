# SurgesEntry Resilience Proof: Screen Recording Guide

This guide explains how to capture a clean terminal demo recording of the **Consumer Kill & Replay** resilience test.

---

## 1. Recommended Screen Recording Tools

- **Windows**:
  - **Xbox Game Bar**: Press `Win + G`, click Record (or `Win + Alt + R`).
  - **OBS Studio**: [obsproject.com](https://obsproject.com/) for custom window cropping.
- **macOS**:
  - **QuickTime Player**: `File` -> `New Screen Recording` -> Select the terminal region.
- **Linux**:
  - **SimpleScreenRecorder** or **OBS Studio**.

---

## 2. Recommended Screen Layout

Open two terminal windows side-by-side:

```text
+---------------------------------------+---------------------------------------+
| LEFT TERMINAL: Live Service Logs      | RIGHT TERMINAL: Automated Demo Script |
|                                       |                                       |
| # Kubernetes mode:                    | pwsh                                  |
| kubectl -n surges-entry logs -f       | cd surges-entry                       |
|   deployment/surges-entry-ingestion   | ./scripts/kill-replay-demo.ps1        |
|                                       |                                       |
| # Or Docker Compose mode:             |                                       |
| docker logs -f surges-entry-ingestion |                                       |
+---------------------------------------+---------------------------------------+
```

---

## 3. Demo Walkthrough

1. **Baseline Ingestion (Events 1-10)**:
   - The script publishes 10 events to Kafka.
   - The left terminal logs `event successfully forwarded to processing service`.
2. **Consumer Crash (Killing Ingestion)**:
   - The script stops or deletes the Ingestion container/pod mid-stream.
   - The left terminal log stream stops.
3. **Outage Buffer (Events 11-15)**:
   - The script publishes 5 more events while Ingestion is offline.
   - Kafka buffers these events on disk without producer errors.
4. **Recovery & Catch-Up**:
   - The container/pod restarts and resumes from the uncommitted offset.
5. **Verification Summary**:
   - The script queries PostgreSQL and confirms:
     - Total published: `15`
     - Total persisted: `15`
     - Missing events: `0`
   - Prints `[PASS] 15/15 events were recovered in this consumer crash-recovery experiment.`.

---

## 4. Saving Recordings

Save exported videos or animated GIFs in `docs/demo/`:
- `docs/demo/kill-replay-demo.mp4` (or `.gif`)
- `docs/demo/offset-replay-demo.mp4` (or `.gif`)
