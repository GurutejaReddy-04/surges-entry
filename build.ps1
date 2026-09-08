# SurgesEntry — Build all 3 microservice Docker images
$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $ScriptDir

$images = @(
    @{ Name = "ingestion"; Path = "services/ingestion/Dockerfile"; Tag = "surges-entry/ingestion:latest" },
    @{ Name = "processing"; Path = "services/processing/Dockerfile"; Tag = "surges-entry/processing:latest" },
    @{ Name = "notification"; Path = "services/notification/Dockerfile"; Tag = "surges-entry/notification:latest" }
)

foreach ($img in $images) {
    Write-Host "Building $($img.Name) Service image ($($img.Tag))..." -ForegroundColor Cyan
    docker build -t $img.Tag -f $img.Path .
}

Write-Host "`nAll SurgesEntry images built successfully!" -ForegroundColor Green
docker images | Select-String "surges-entry"
