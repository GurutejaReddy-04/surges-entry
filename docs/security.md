# Security & Secret Management

> *SurgesEntry Distributed Event Processing Platform*

---

## 1. Secrets Architecture & Decoupling

SurgesEntry enforces strict separation between source code, deployment templates, and runtime credentials.

```text
       Developer / CI                     Production Kubernetes
             |                                      |
    +--------+--------+                    +--------+--------+
    v                 v                    v                 v
.env.example    secret.example.yaml   HashiCorp Vault   AWS Secrets Manager
 (Template)        (Template)              |                 |
    |                 |                    +--------+--------+
    v                 v                             v
  .env          k8s/secret.yaml        External Secrets Operator (ESO)
(Git-Ignored)    (Git-Ignored)                      |
                                                    v
                                       Ephemeral Kubernetes Secret
```

### 1.1 Local Development Isolation
1. **Templates Only in Source Control**:
   - `.env.example`: Self-documenting template for local Docker Compose variables.
   - `k8s/secret.example.yaml`: Placeholder definitions for `POSTGRES_DSN` and `INTERNAL_AUTH_TOKEN`.
2. **Ignored Operational Secrets**:
   - `.gitignore` excludes `.env`, `.env.*`, and `k8s/secret.yaml`.
3. **Fail-Closed Configuration**:
   - `docker-compose.yml` requires `POSTGRES_USER` and `POSTGRES_PASSWORD` via Compose variable checks (`:?`).
   - Processing Service exits immediately on startup if `POSTGRES_DSN` is unset.

### 1.2 Inter-Service gRPC Authentication
Both `ProcessingService` and `NotificationService` enforce shared-token validation via gRPC unary server interceptors when `INTERNAL_AUTH_TOKEN` is configured:
- Outgoing gRPC clients (`Ingestion` -> `Processing`, `Processing` -> `Notification`) attach `x-surges-internal-token` metadata.
- Server interceptors validate the token using constant-time comparison (`crypto/subtle.ConstantTimeCompare`), returning `codes.Unauthenticated` or `codes.PermissionDenied` on mismatch while exempting `/grpc.health.v1.Health/` probes.
- Panic recovery unary interceptors catch handler panics and return `codes.Internal` without crashing the gRPC server.

### 1.3 Continuous Integration (CI) Ephemeral Test Credentials
Integration tests in GitHub Actions (`.github/workflows/ci.yml`) run against ephemeral Docker containers using disposable runner-scoped credentials (`eventplatform_ci_password`).

---

## 2. Container Sandboxing & Least Privilege

All microservice containers (`ingestion`, `processing`, `notification`) follow defense-in-depth practices:

### 2.1 Multi-Stage Minimal Images
* Built with `golang:1.23-alpine` and `CGO_ENABLED=0` to produce statically linked binaries.
* Runtime images run on minimal `alpine:3.20` with an image footprint under 40 MB.

### 2.2 Pod Security Context (Kubernetes)
Configured across all deployment manifests:
```yaml
securityContext:
  runAsNonRoot: true
  runAsUser: 10001
  runAsGroup: 10001
  fsGroup: 10001
containers:
  - name: surges-entry-processing
    securityContext:
      allowPrivilegeEscalation: false
      readOnlyRootFilesystem: true
      capabilities:
        drop:
          - ALL
```
* **Non-Root Execution**: Runs as dedicated `appuser` (UID/GID 10001).
* **Read-Only Root Filesystem**: Immutable root filesystem with `/tmp` backed by an `emptyDir` volume.
* **Linux Capabilities Dropped**: All Linux capabilities dropped (`drop: [ALL]`).

---

## 3. Enterprise Production Recommendations

In production cloud environments:
1. **External Secrets Operator (ESO)**: Sync secrets dynamically from AWS Secrets Manager, HashiCorp Vault, or Azure Key Vault instead of static Secret manifests.
2. **Managed IAM Database Auth**: Replace static PostgreSQL passwords with short-lived IAM authentication tokens where supported.
3. **Mutual TLS (mTLS)**: Terminate and rotate inter-service mTLS certificates using a service mesh (Istio/Linkerd) or SPIFFE/SPIRE.
