---
permalink: /index.html
---

# SurgesEntry — Distributed Event Processing Platform
> *Production-hardened distributed event processing platform in Go with Kafka, gRPC, Redis, and Kubernetes.*

Welcome to the **SurgesEntry** documentation site.

[View the complete project README on GitHub](https://github.com/GurutejaReddy-04/surges-entry)

---

## Technical Documentation

- **[Architecture Overview](architecture.md)**: Deep dive into microservice roles, Kafka partition affinity, Redis atomic sliding windows, two-tier anomaly detection, and distributed tracing.
- **[Resilience Proof & Recording Guide](demo/RECORDING_GUIDE.md)**: Terminal split-layout guide and instructions for consumer crash-recovery and offset replay verification.
- **[Protocol Buffer Contracts](https://github.com/GurutejaReddy-04/surges-entry/blob/master/proto/event.proto)**: Protobuf service definitions and schemas for inter-service gRPC communication.
- **[Kubernetes Deployment Guide](https://github.com/GurutejaReddy-04/surges-entry/blob/master/k8s/README.md)**: Manifests, security contexts, probes, and manual scaling instructions.
