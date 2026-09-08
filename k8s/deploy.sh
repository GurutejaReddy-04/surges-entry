#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=== Phase 6: Kubernetes Deployment ==="

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
    minikube image load event-platform/ingestion:latest
    minikube image load event-platform/processing:latest
    minikube image load event-platform/notification:latest
    echo "Images loaded into Minikube!"
fi

echo "Applying Kubernetes manifests..."
kubectl apply -f namespace.yaml
kubectl apply -f secret.yaml
kubectl apply -f configmap.yaml
kubectl apply -f deployment-notification.yaml
kubectl apply -f service-notification.yaml
kubectl apply -f deployment-processing.yaml
kubectl apply -f service-processing.yaml
kubectl apply -f deployment-ingestion.yaml
kubectl apply -f service-ingestion.yaml

echo "Waiting for pods to be Ready (timeout: 120s)..."
kubectl -n event-platform wait --for=condition=ready pod -l app=notification-service --timeout=120s
kubectl -n event-platform wait --for=condition=ready pod -l app=processing-service --timeout=120s
kubectl -n event-platform wait --for=condition=ready pod -l app=ingestion-service --timeout=120s

echo ""
echo "=== Deployment Status ==="
kubectl -n event-platform get pods -o wide
echo ""
kubectl -n event-platform get svc -o wide

echo ""
echo "All 3 microservices are deployed and healthy in Kubernetes!"
