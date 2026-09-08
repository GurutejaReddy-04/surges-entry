#!/usr/bin/env pwsh
# Phase 1 verification: container health, Kafka CLI, simulator, partition affinity.
# Run from the event-platform/ directory after `docker-compose up -d`.

param(
    [int]$SimulatorSeconds = 15
)

$ErrorActionPreference = "Continue"
$script:failed = 0

function Step([string]$n, [string]$label) {
    Write-Host "`n--- [$n] $label ---" -ForegroundColor Cyan
}
function Pass([string]$msg) { Write-Host "  PASS  $msg" -ForegroundColor Green }
function Fail([string]$msg) { $script:failed++; Write-Host "  FAIL  $msg" -ForegroundColor Red }

# ── 1. Container health ──────────────────────────────────────────────────────
Step "1/4" "Container health"

$containers = @(
    @{ Name = "kafka";    HasHealthcheck = $true  },
    @{ Name = "redis";    HasHealthcheck = $true  },
    @{ Name = "postgres"; HasHealthcheck = $true  },
    @{ Name = "jaeger";   HasHealthcheck = $true  }
)

foreach ($c in $containers) {
    $name = $c.Name
    if ($c.HasHealthcheck) {
        $status = docker inspect --format '{{.State.Health.Status}}' $name 2>$null
        if ($status -eq "healthy") { Pass "$name is healthy" }
        else { Fail "$name status: $status (expected: healthy)" }
    } else {
        $running = docker inspect --format '{{.State.Running}}' $name 2>$null
        if ($running -eq "true") { Pass "$name is running" }
        else { Fail "$name is not running" }
    }
}

# ── 2. Kafka CLI round-trip ──────────────────────────────────────────────────
Step "2/4" "Kafka CLI produce/consume"

$marker = "verify-$(Get-Date -Format 'yyyyMMdd-HHmmss')"
docker exec kafka bash -c "echo '$marker' | kafka-console-producer --bootstrap-server localhost:9092 --topic verify-roundtrip 2>/dev/null" 2>$null
Start-Sleep -Seconds 2

$consumed = docker exec kafka bash -c "kafka-console-consumer --bootstrap-server localhost:9092 --topic verify-roundtrip --from-beginning --timeout-ms 5000 2>/dev/null" 2>$null

if ($consumed -match [regex]::Escape($marker)) {
    Pass "Kafka round-trip: produced and consumed '$marker'"
} else {
    Fail "Message '$marker' not consumed back"
}

# ── 3. Simulator smoke test ──────────────────────────────────────────────────
Step "3/4" "Simulator ($SimulatorSeconds seconds)"

$outFile = Join-Path $PSScriptRoot "simulator_verify_out.txt"
$errFile = Join-Path $PSScriptRoot "simulator_verify_err.txt"

$proc = Start-Process -FilePath "go" `
    -ArgumentList "run", "." `
    -WorkingDirectory (Join-Path $PSScriptRoot "simulator") `
    -RedirectStandardOutput $outFile `
    -RedirectStandardError $errFile `
    -PassThru -NoNewWindow

Start-Sleep -Seconds $SimulatorSeconds
Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue 2>$null
Start-Sleep -Seconds 1

$lines = @()
if (Test-Path $outFile) {
    $lines = Get-Content $outFile
}

$publishedLines = $lines | Where-Object { $_ -match '"msg":"published"' }
$count = $publishedLines.Count

if ($count -gt 0) {
    Pass "Simulator published $count events in $SimulatorSeconds seconds"
} else {
    Fail "No events published. Check $(Split-Path $errFile -Leaf)"
    if (Test-Path $errFile) {
        $errContent = Get-Content $errFile -Raw
        if ($errContent) { Write-Host "  stderr: $errContent" -ForegroundColor DarkYellow }
    }
}

# ── 4. Partition affinity ────────────────────────────────────────────────────
Step "4/4" "Partition affinity"

$partitionMap = @{}
$violations = 0

foreach ($line in $publishedLines) {
    try {
        $obj = $line | ConvertFrom-Json
        $uid = $obj.user_id
        $part = [int]$obj.partition

        if ($partitionMap.ContainsKey($uid) -and $partitionMap[$uid] -ne $part) {
            Fail "$uid moved from partition $($partitionMap[$uid]) to $part"
            $violations++
        }
        $partitionMap[$uid] = $part
    } catch {
        # skip malformed lines
    }
}

if ($violations -eq 0 -and $partitionMap.Count -gt 0) {
    Pass "All $($partitionMap.Count) users consistently mapped to their partition"
    foreach ($kv in $partitionMap.GetEnumerator() | Sort-Object Name) {
        Write-Host "    $($kv.Name) -> partition $($kv.Value)" -ForegroundColor DarkGray
    }
} elseif ($partitionMap.Count -eq 0) {
    Fail "No partition data to verify (no published events?)"
}

# ── Cleanup ──────────────────────────────────────────────────────────────────
Remove-Item $outFile -ErrorAction SilentlyContinue
Remove-Item $errFile -ErrorAction SilentlyContinue

# ── Summary ──────────────────────────────────────────────────────────────────
Write-Host "`n=== Phase 1 Verification ===" -ForegroundColor Cyan
if ($script:failed -eq 0) {
    Write-Host "All checks passed." -ForegroundColor Green
    exit 0
} else {
    Write-Host "$($script:failed) check(s) failed." -ForegroundColor Red
    exit 1
}
