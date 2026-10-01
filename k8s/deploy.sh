#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=== SurgesEntry: Kubernetes Deployment ==="

if ! command -v kubectl &> /dev/null; then
    echo "Error: kubectl not found on PATH." >&2
    exit 1
fi

echo "Checking Kubernetes cluster connection..."
kubectl cluster-info

CURRENT_CONTEXT="$(kubectl config current-context 2>/dev/null || true)"
echo "Current Kubernetes context: ${CURRENT_CONTEXT}"

if [[ "${CURRENT_CONTEXT}" == *"minikube"* ]]; then
    echo "Loading images into Minikube..."
    minikube image load surges-entry/ingestion:latest
    minikube image load surges-entry/processing:latest
    minikube image load surges-entry/notification:latest
    echo "Images loaded into Minikube!"
fi

echo "Applying Kubernetes manifests..."
kubectl apply -f namespace.yaml

if [ -f "secret.yaml" ]; then
    echo "Applying secret from secret.yaml..."
    kubectl apply -f secret.yaml
elif kubectl -n surges-entry get secret platform-secrets &> /dev/null; then
    echo "Reusing existing 'platform-secrets' Secret in surges-entry namespace."
else
    echo "Error: Secret 'platform-secrets' not found in namespace 'surges-entry' and 'k8s/secret.yaml' does not exist." >&2
    echo "To configure:" >&2
    echo "  1. Copy k8s/secret.example.yaml to k8s/secret.yaml and supply your credentials, OR" >&2
    echo "  2. Provision externally: kubectl -n surges-entry create secret generic platform-secrets --from-literal=POSTGRES_DSN=\"...\"" >&2
    exit 1
fi

kubectl apply -f configmap.yaml
kubectl apply -f deployment-notification.yaml
kubectl apply -f service-notification.yaml
kubectl apply -f deployment-processing.yaml
kubectl apply -f service-processing.yaml
kubectl apply -f deployment-ingestion.yaml
kubectl apply -f service-ingestion.yaml

echo "Waiting for pods to be Ready (timeout: 120s)..."
kubectl -n surges-entry wait --for=condition=ready pod -l app=surges-entry-notification --timeout=120s
kubectl -n surges-entry wait --for=condition=ready pod -l app=surges-entry-processing --timeout=120s
kubectl -n surges-entry wait --for=condition=ready pod -l app=surges-entry-ingestion --timeout=120s

echo ""
echo "=== Deployment Status ==="
kubectl -n surges-entry get pods -o wide
echo ""
kubectl -n surges-entry get svc -o wide

echo ""
echo "All 3 microservices are deployed and healthy in Kubernetes!"
