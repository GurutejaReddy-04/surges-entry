# Resilience Proof: Screen Recording Guide

This guide explains how to capture a clean, professional demo recording of the **Consumer Kill & Replay** resilience test for portfolio showcases or technical interviews.

---

## 1. Recommended Screen Recording Tools

- **Windows**:
  - **Xbox Game Bar**: Press `Win + G`, click the Record button (or shortcut `Win + Alt + R`).
  - **OBS Studio** (Free & Open Source): [obsproject.com](https://obsproject.com/) — recommended for custom window cropping.
- **macOS**:
  - **QuickTime Player**: `File` → `New Screen Recording` → Select the terminal region.
- **Linux**:
  - **SimpleScreenRecorder** or **OBS Studio**.

---

## 2. Recommended Screen Layout

Open two terminal windows side-by-side:

```
┌───────────────────────────────────────┬───────────────────────────────────────┐
│ LEFT TERMINAL: Live Service Logs      │ RIGHT TERMINAL: Automated Demo Script │
│                                       │                                       │
│ # Kubernetes mode:                    │ powershell                            │
│ kubectl -n event-platform logs -f     │ cd event-platform                     │
│   deployment/ingestion-service        │ ./scripts/kill-replay-demo.ps1        │
│                                       │                                       │
│ # Or Docker Compose mode:             │                                       │
│ docker logs -f ingestion              │                                       │
└───────────────────────────────────────┴───────────────────────────────────────┘
```

---

## 3. What Happens in the Demo (Storyline)

1. **Baseline Ingestion (Events 1–10)**:
   - The script publishes 10 events to Kafka.
   - The left terminal shows active `event successfully forwarded to processing service` logs.
2. **The Sudden Crash (Killing Ingestion)**:
   - The script targets the Ingestion Service pod/container and issues a hard termination.
   - The left terminal stream halts immediately.
3. **Outage Buffer (Events 11–15)**:
   - The script sends 5 more events while Ingestion is **completely offline**.
   - Proves Kafka decouples producers from consumers: producers never receive backpressure or dropped requests.
4. **Self-Healing & Catch-Up**:
   - The container / Kubernetes pod restarts automatically.
   - The left terminal reconnects to Kafka and resumes consuming from the exact uncommitted offset.
5. **Verification Table**:
   - The script queries PostgreSQL directly and calculates:
     - Total published: `15`
     - Total persisted: `15`
     - Missing events: `0`
   - Prints `🚀 PASS: ZERO DATA LOSS CONFIRMED!`.

---

## 4. Saving the Recording

Save the exported video or animated GIF in `docs/demo/`:
- `docs/demo/kill-replay-demo.mp4` (or `.gif`)
- `docs/demo/offset-replay-demo.mp4` (or `.gif`)
