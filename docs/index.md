---
permalink: /index.html
---

# SurgesEntry — Distributed Event Processing Platform
> *Distributed event processing platform in Go with Kafka, gRPC, Redis, and Kubernetes.*

Welcome to the **SurgesEntry** technical documentation hub.

[View the complete project README on GitHub](https://github.com/GurutejaReddy-04/surges-entry)

---

## Technical Documentation Guides

- **[System Architecture](architecture.md)**: Deep dive into microservice roles, data flow, implemented local hybrid demo vs. reference cloud production architecture, and architectural trade-offs.
- **[Reliability & Fault-Tolerance](reliability.md)**: At-least-once delivery contract, poison-pill drop-and-log boundary, consumer crash resilience experiments, and defensive offset commit mechanics.
- **[Performance & Latency](performance.md)**: Redis `TxPipeline` atomic pre-write evaluation, outlier self-pollution defense, asynchronous worker pools, and trace flame graph analysis.
- **[Benchmark Methodology & Provenance](../BENCHMARKS.md)**: Reconstructed experimental setup, simulator configuration gaps (`EVENT_RATE=200`), and step-by-step reproduction instructions.
- **[Security & Secret Management](security.md)**: Secret decoupling (`.env.example`, `secret.example.yaml`), non-root container sandboxing, historical credential audit, and enterprise secrets architecture.
- **[Kubernetes Deployment Guide](../k8s/README.md)**: Manifests, security contexts, probes, secret configuration, and manual scaling instructions.
- **[Resilience Proof & Recording Guide](demo/RECORDING_GUIDE.md)**: Terminal split-layout guide and instructions for consumer crash-recovery and offset replay verification.
- **[Protocol Buffer Contracts](https://github.com/GurutejaReddy-04/surges-entry/blob/master/proto/event.proto)**: Protobuf service definitions and schemas for inter-service gRPC communication.
