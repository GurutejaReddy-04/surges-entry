# Deploy Distributed Event Processing Platform to Kubernetes
$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $ScriptDir

Write-Host "=== Phase 6: Kubernetes Deployment ===" -ForegroundColor Cyan

# 1. Verify cluster connectivity
if (-not (Get-Command kubectl -ErrorAction SilentlyContinue)) {
    Write-Error "kubectl is not found on PATH."
    exit 1
}

Write-Host "Checking Kubernetes cluster connection..." -ForegroundColor Yellow
$clusterInfo = kubectl cluster-info 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Error "Cannot connect to a running Kubernetes cluster. Please enable Kubernetes in Docker Desktop or run 'minikube start'."
    exit 1
}
Write-Host "Cluster connection verified!" -ForegroundColor Green

# 2. If running Minikube, load local images into Minikube's Docker daemon
$currentContext = (kubectl config current-context).Trim()
Write-Host "Current Kubernetes context: $currentContext" -ForegroundColor Yellow

if ($currentContext -match "minikube") {
    Write-Host "Loading images into Minikube..." -ForegroundColor Yellow
    minikube image load event-platform/ingestion:latest
    minikube image load event-platform/processing:latest
    minikube image load event-platform/notification:latest
    Write-Host "Images loaded into Minikube!" -ForegroundColor Green
}

# 3. Apply manifests
Write-Host "`nApplying Kubernetes manifests..." -ForegroundColor Cyan
kubectl apply -f namespace.yaml
kubectl apply -f secret.yaml
kubectl apply -f configmap.yaml
kubectl apply -f deployment-notification.yaml
kubectl apply -f service-notification.yaml
kubectl apply -f deployment-processing.yaml
kubectl apply -f service-processing.yaml
kubectl apply -f deployment-ingestion.yaml
kubectl apply -f service-ingestion.yaml

# 4. Wait for rollouts
Write-Host "`nWaiting for pods to be Ready (timeout: 120s)..." -ForegroundColor Yellow
kubectl -n event-platform wait --for=condition=ready pod -l app=notification-service --timeout=120s
kubectl -n event-platform wait --for=condition=ready pod -l app=processing-service --timeout=120s
kubectl -n event-platform wait --for=condition=ready pod -l app=ingestion-service --timeout=120s

Write-Host "`n=== Deployment Status ===" -ForegroundColor Green
kubectl -n event-platform get pods -o wide
Write-Host ""
kubectl -n event-platform get svc -o wide

Write-Host "`nAll 3 microservices are deployed and healthy in Kubernetes!" -ForegroundColor Green
