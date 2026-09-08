# Ensure Go tools and protoc are on PATH
$env:PATH = [System.Environment]::GetEnvironmentVariable("Path","Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path","User") + ";C:\Program Files\Go\bin;$env:USERPROFILE\go\bin;" + $env:PATH

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $ScriptDir

if (-not (Get-Command protoc -ErrorAction SilentlyContinue)) {
    Write-Error "protoc not found on PATH. Please install protobuf compiler."
    exit 1
}

if (-not (Get-Command protoc-gen-go -ErrorAction SilentlyContinue)) {
    Write-Host "Installing protoc-gen-go..."
    go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
}

if (-not (Get-Command protoc-gen-go-grpc -ErrorAction SilentlyContinue)) {
    Write-Host "Installing protoc-gen-go-grpc..."
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
}

$outDir = Join-Path $ScriptDir "gen\event"
if (-not (Test-Path $outDir)) {
    New-Item -ItemType Directory -Path $outDir -Force | Out-Null
}

protoc --go_out=. --go_opt=module=event-platform/proto --go-grpc_out=. --go-grpc_opt=module=event-platform/proto event.proto

if ($LASTEXITCODE -eq 0) {
    Write-Host "Successfully generated gRPC stubs in proto/gen/event" -ForegroundColor Green
} else {
    Write-Error "protoc generation failed with exit code $LASTEXITCODE"
    exit $LASTEXITCODE
}
