# Deploy SurgesEntry to Kubernetes
$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $ScriptDir

Write-Host "=== SurgesEntry: Kubernetes Deployment ===" -ForegroundColor Cyan

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
    minikube image load surges-entry/ingestion:latest
    minikube image load surges-entry/processing:latest
    minikube image load surges-entry/notification:latest
    Write-Host "Images loaded into Minikube!" -ForegroundColor Green
}

# 3. Apply manifests
Write-Host "`nApplying Kubernetes manifests..." -ForegroundColor Cyan
kubectl apply -f namespace.yaml

if (Test-Path "secret.yaml") {
    Write-Host "Applying secret from secret.yaml..." -ForegroundColor Cyan
    kubectl apply -f secret.yaml
} else {
    $existingSecret = kubectl -n surges-entry get secret platform-secrets 2>$null
    if ($LASTEXITCODE -eq 0 -and $existingSecret) {
        Write-Host "Reusing existing 'platform-secrets' Secret in namespace 'surges-entry'." -ForegroundColor Green
    } else {
        Write-Error "Secret 'platform-secrets' not found in namespace 'surges-entry' and 'k8s/secret.yaml' does not exist.`nTo configure:`n  1. Copy k8s/secret.example.yaml to k8s/secret.yaml and supply your credentials, OR`n  2. Provision externally: kubectl -n surges-entry create secret generic platform-secrets --from-literal=POSTGRES_DSN=`"...`""
        exit 1
    }
}

kubectl apply -f configmap.yaml
kubectl apply -f deployment-notification.yaml
kubectl apply -f service-notification.yaml
kubectl apply -f deployment-processing.yaml
kubectl apply -f service-processing.yaml
kubectl apply -f deployment-ingestion.yaml
kubectl apply -f service-ingestion.yaml

# 4. Wait for rollouts
Write-Host "`nWaiting for pods to be Ready (timeout: 120s)..." -ForegroundColor Yellow
kubectl -n surges-entry wait --for=condition=ready pod -l app=surges-entry-notification --timeout=120s
kubectl -n surges-entry wait --for=condition=ready pod -l app=surges-entry-processing --timeout=120s
kubectl -n surges-entry wait --for=condition=ready pod -l app=surges-entry-ingestion --timeout=120s

Write-Host "`n=== Deployment Status ===" -ForegroundColor Green
kubectl -n surges-entry get pods -o wide
Write-Host ""
kubectl -n surges-entry get svc -o wide

Write-Host "`nAll 3 microservices are deployed and healthy in Kubernetes!" -ForegroundColor Green
