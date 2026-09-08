# Build all 3 microservice Docker images
$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $ScriptDir

Write-Host "Building Ingestion Service image..." -ForegroundColor Cyan
docker build -t event-platform/ingestion:latest -f services/ingestion/Dockerfile .

Write-Host "Building Processing Service image..." -ForegroundColor Cyan
docker build -t event-platform/processing:latest -f services/processing/Dockerfile .

Write-Host "Building Notification Service image..." -ForegroundColor Cyan
docker build -t event-platform/notification:latest -f services/notification/Dockerfile .

Write-Host "`nAll images built successfully!" -ForegroundColor Green
docker images | Select-String "event-platform"
