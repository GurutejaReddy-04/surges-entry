# Security & Secret Management

> *SurgesEntry Distributed Event Processing Platform*

---

## 1. Secrets Architecture & Decoupling

SurgesEntry enforces strict separation between application code, public manifests, and operational secrets.

```
       Developer / CI                     Production Kubernetes
             │                                      │
    ┌────────┴────────┐                    ┌────────┴────────┐
    ▼                 ▼                    ▼                 ▼
.env.example    secret.example.yaml   HashiCorp Vault   AWS Secrets Manager
 (Template)        (Template)               │                 │
    │                 │                     └────────┬────────┘
    ▼                 ▼                              ▼
  .env          k8s/secret.yaml         External Secrets Operator (ESO)
(Git-Ignored)    (Git-Ignored)                       │
                                                     ▼
                                        Ephemeral Kubernetes Secret
```

### 1.1 Local Development Isolation
1. **Templates Only in Source Control**:
   - `.env.example`: Provides a self-documenting template for local Docker Compose variables.
   - `k8s/secret.example.yaml`: Provides placeholder definitions for Kubernetes deployments.
2. **Ignored Operational Secrets**:
   - `.gitignore` explicitly excludes `.env`, `.env.*`, and `k8s/secret.yaml`.
3. **Fail-Closed Configuration**:
   - `docker-compose.yml` uses strict variable validation (`${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}`). If `.env` is omitted, Compose halts immediately rather than launching unauthenticated or default-password services.
   - Microservices validate database connection configuration on startup and exit immediately if required credentials are not supplied.

### 1.2 Continuous Integration (CI) Ephemeral Test Credentials
Integration tests executed in GitHub Actions run against disposable ephemeral service containers (Kafka KRaft, Redis 7, PostgreSQL 16). The CI workflow configuration (`.github/workflows/ci.yml`) provisions an isolated, throwaway runner password (`POSTGRES_PASSWORD: eventplatform_ci_password`) scoped strictly to the short-lived runner virtual machine. No active production, staging, or developer workstation credentials are committed to version control.

---

## 2. Container Sandboxing & Least Privilege

All microservice containers (`ingestion`, `processing`, `notification`) follow defense-in-depth container security practices:

### 2.1 Multi-Stage Minimal Images
* Built with `CGO_ENABLED=0` producing statically linked Go binaries.
* Executed on minimal Alpine 3.20 base images with an image footprint under 40 MB.

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
* **Non-Root Execution**: Runs as dedicated `appuser` (UID/GID 10001), preventing privilege escalation to host root.
* **Read-Only Root Filesystem**: The container filesystem is immutable. Temporary write scratchpads are restricted to an explicitly mounted in-memory `/tmp` volume.
* **Linux Capabilities Dropped**: All Linux capabilities (`CAP_SYS_ADMIN`, `CAP_NET_RAW`, etc.) are dropped.

---

## 3. Historical Credential Audit & Git History Classification

### 3.1 Audit Findings
During security review of the repository, the static development credentials (`eventplatform:eventplatform`) were identified in:
* Git commits `799e3c8` and `454ba20` touching `k8s/secret.yaml` and `docker-compose.yml`.

### 3.2 Classification & Threat Assessment
* **Classification**: These credentials represent **disposable local-development placeholders** generated strictly for offline Docker Compose and local Minikube validation.
* **Exposure Risk**: Because they have never been applied to any public database, cloud cluster, or production environment, no live cloud infrastructure was exposed.
* **Mandatory Rotation Policy**: **If these credentials were ever copied, reused, or connected to any external database or non-disposable staging/production instance, they MUST be rotated immediately.**

### 3.3 Purging Historical Commits (Optional History Rewriting)
Because removing a file from `master` does not remove it from Git history objects, maintainers desiring a clean public commit history may rewrite past commits using [`git-filter-repo`](https://github.com/newren/git-filter-repo):

```bash
# 1. Install git-filter-repo
pip install git-filter-repo

# 2. Purge secret.yaml from entire commit history
git filter-repo --invert-paths --path k8s/secret.yaml

# 3. Force-push rewritten branches to remote
git push origin --force --all
```

---

## 4. Enterprise Production Secrets Management

In production cloud environments, static Kubernetes Secret files should not be used. The recommended reference architecture incorporates:

1. **External Secrets Operator (ESO)**:
   - Microservices declare an `ExternalSecret` custom resource.
   - The ESO controller polls AWS Secrets Manager, HashiCorp Vault, or Azure Key Vault and synchronizes secrets into ephemeral in-memory Kubernetes `Secret` objects.
2. **Cloud IAM Database Authentication**:
   - For PostgreSQL (AWS Aurora / GCP Cloud SQL), replace password authentication entirely with short-lived (15-minute) IAM authentication tokens generated by the AWS/GCP SDK.
3. **Mutual TLS (mTLS)**:
   - Inter-service gRPC communication is encrypted in transit using SPIFFE/SPIRE certificates or an Istio/Linkerd service mesh sidecar proxy.
