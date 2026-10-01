# SurgesEntry — Kubernetes Deployment

Stateless Go microservices deployed into Kubernetes (`surges-entry` namespace) with health probes, horizontal scaling, and ConfigMap routing to external stateful dependencies.

---

## Architecture & Networking Strategy

Stateful infrastructure (Kafka KRaft, Redis, PostgreSQL, Jaeger) runs externally in Docker Compose on the host. This avoids managing in-cluster StatefulSets and PersistentVolumes for storage engines while showcasing production-style microservice orchestration.

```
Host (Docker Compose)                   Kubernetes Cluster (surges-entry namespace)
┌───────────────────────────┐           ┌──────────────────────────────────────────────┐
│  Kafka      (:9092)       │ ◄──────── │  Ingestion Pods   (Deployment)               │
│  Redis      (:6379)       │ ◄────┐    │        │ (gRPC)                              │
│  Postgres   (:5432)       │ ◄──┐ └─── │  Processing Pods  (Deployment, replicas: 1-3)│
│  Jaeger     (:4317/:16686)│    └───── │        │ (gRPC)                              │
└───────────────────────────┘           │  Notification Pod (Deployment)               │
                                        └──────────────────────────────────────────────┘
```

### Host Reachability:
1. **Docker Desktop Kubernetes (Windows / macOS)**:
   - Pods connect to host dependencies using `host.docker.internal` (configured in `configmap.yaml`).
2. **Minikube (Docker Driver)**:
   - When using Minikube with `--driver=docker`, use `host.minikube.internal` or the bridge gateway IP.

---

## Manifests

| File | Type | Purpose |
|---|---|---|
| `namespace.yaml` | Namespace | Creates `surges-entry` namespace |
| `configmap.yaml` | ConfigMap | Environment variables for Kafka, Redis, Postgres, and service URLs |
| `secret.example.yaml` | Secret Template | Placeholder template for PostgreSQL credentials (copy to untracked `secret.yaml` or inject externally) |
| `deployment-notification.yaml` | Deployment | Runs `surges-entry-notification` with readiness/liveness probes |
| `service-notification.yaml` | Service | ClusterIP exposing `:50052` (gRPC) and `:8080` (health) |
| `deployment-processing.yaml` | Deployment | Runs `surges-entry-processing` (1–3 replicas) |
| `service-processing.yaml` | Service | ClusterIP exposing `:50051` (gRPC) and `:8080` (health) |
| `deployment-ingestion.yaml` | Deployment | Runs `surges-entry-ingestion` consuming from Kafka |
| `service-ingestion.yaml` | Service | ClusterIP exposing `:8080` (health) |

---

## Probes & Self-Healing

Each service implements `grpc.health.v1` and exposes an internal `/healthz` HTTP probe endpoint on port `8080` that verifies gRPC serving status:
- **Readiness Probe**: Delays traffic until dependencies are initialized.
- **Liveness Probe**: Automatically restarts failed or deadlocked pods.

---

## Deployment Instructions

### 1. Ensure Docker Compose dependencies are up
```powershell
docker-compose up -d
```

### 2. Configure Kubernetes Secrets
```powershell
# Copy the example secret template to local untracked secret.yaml
Copy-Item secret.example.yaml secret.yaml   # On Linux: cp secret.example.yaml secret.yaml

# Edit secret.yaml with your database credentials (or provide via External Secrets Operator / Vault)
```

### 3. Enable Kubernetes
- **Option A (Docker Desktop)**: Settings -> Kubernetes -> Check **Enable Kubernetes** -> Apply & restart.
- **Option B (Minikube)**:
  ```powershell
  minikube start --driver=docker
  ```

### 4. Deploy
```powershell
cd k8s
./deploy.ps1  # or ./deploy.sh
```

---

## Manual Scaling Demonstration

Redis `TxPipeline` atomicity guarantees hold across multiple concurrent processing replicas:

```powershell
# Scale processing service to 3 replicas
kubectl -n surges-entry scale deployment/surges-entry-processing --replicas=3

# Verify all 3 pods are running and ready
kubectl -n surges-entry get pods -l app=surges-entry-processing

# Watch concurrent load distribution across pods
kubectl -n surges-entry logs -f -l app=surges-entry-processing --max-log-requests=10 --tail=20
```
