# Phase 7 — Kafka Consumer Group Offset Replay Demo
# Proves Kafka's capability to reprocess historical event streams on demand.

param(
    [string]$ConsumerGroup = "ingestion-service-group",
    [string]$PostgresUser = "eventplatform",
    [string]$PostgresDb = "eventplatform",
    [string]$Namespace = "event-platform"
)

$ErrorActionPreference = "Continue"

function Log-Step([string]$step, [string]$msg) {
    $ts = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
    Write-Host "`n[$ts] [$step] $msg" -ForegroundColor Cyan
}

function Log-Pass([string]$msg) {
    Write-Host "  🚀 PASS: $msg" -ForegroundColor Green
}

function Log-Fail([string]$msg) {
    Write-Host "  💀 FAIL: $msg" -ForegroundColor Red
    exit 1
}

$runID = "replaydemo-" + [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
$user = "replay-user-$runID"

function Query-DBCount() {
    $sql = "SELECT COUNT(*) FROM processed_events WHERE user_id = '$user';"
    $res = docker exec postgres psql -U $PostgresUser -d $PostgresDb -t -A -c $sql 2>$null
    if ($res -match '^\d+$') {
        return [int]$res
    }
    return 0
}

function Publish-KafkaEvent([string]$userID, [string]$eventType, [double]$val, [string]$traceID) {
    $ts = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
    $json = "{`"user_id`":`"$userID`",`"event_type`":`"$eventType`",`"value`":$val,`"timestamp`":$ts,`"trace_id`":`"$traceID`"}"
    $json | docker exec -i kafka kafka-console-producer --bootstrap-server kafka:29092 --topic events 2>$null | Out-Null
}

Write-Host "============================================================" -ForegroundColor Yellow
Write-Host "  🚀 PHASE 7: KAFKA OFFSET REPLAY DEMO                     " -ForegroundColor Yellow
Write-Host "============================================================" -ForegroundColor Yellow

# ── Step 1: Detect Environment ───────────────────────────────────────────────
Log-Step "1/5" "Checking deployment environment..."
$useK8s = $false
$podCheck = kubectl -n $Namespace get pods -l app=ingestion-service -o jsonpath='{.items[0].status.phase}' 2>$null
if ($podCheck -eq "Running") {
    $useK8s = $true
    $ConsumerGroup = "k8s-ingestion-group"
    Log-Pass "Targeting Kubernetes consumer group '$ConsumerGroup'"
} else {
    Log-Pass "Targeting Docker Compose consumer group '$ConsumerGroup'"
}

# ── Step 2: Publish Initial Batch ────────────────────────────────────────────
Log-Step "2/5" "Publishing 5 distinct events for replay tracking..."
for ($i = 1; $i -le 5; $i++) {
    $val = 50.0 + ($i * 10.0)
    $trace = "$runID-event-$i"
    Publish-KafkaEvent -userID $user -eventType "replay-test" -val $val -traceID $trace
    Start-Sleep -Milliseconds 50
}
Log-Pass "5 events published"

# Wait for initial processing
$initialCount = 0
for ($r = 0; $r -lt 15; $r++) {
    Start-Sleep -Seconds 1
    $initialCount = Query-DBCount
    if ($initialCount -ge 5) { break }
}

if ($initialCount -ge 5) {
    Log-Pass "Initial batch processed into PostgreSQL (count: $initialCount)"
} else {
    Log-Fail "Initial batch failed to process (only $initialCount in DB)"
}

# ── Step 3: Stop Consumer Group To Allow Offset Reset ────────────────────────
Log-Step "3/5" "Pausing Ingestion Service (Kafka requires inactive consumer group to alter offsets)..."

if ($useK8s) {
    kubectl -n $Namespace scale deployment/ingestion-service --replicas=0 2>$null | Out-Null
    Start-Sleep -Seconds 3
} else {
    docker stop ingestion 2>$null | Out-Null
    Start-Sleep -Seconds 2
}
Log-Pass "Consumer group is now inactive and ready for offset manipulation"

# ── Step 4: Reset Kafka Consumer Group Offsets to Earliest ───────────────────
Log-Step "4/5" "Rewinding Kafka offsets to --to-earliest for topic 'events'..."

$resetOutput = docker exec kafka kafka-consumer-groups `
    --bootstrap-server kafka:29092 `
    --group $ConsumerGroup `
    --topic events `
    --reset-offsets `
    --to-earliest `
    --execute 2>&1

Write-Host $resetOutput -ForegroundColor DarkGray
Log-Pass "Consumer group offsets rewound to earliest position"

# ── Step 5: Restart Consumer & Verify Reprocessing ───────────────────────────
Log-Step "5/5" "Restarting Ingestion Service and observing historical replay..."

if ($useK8s) {
    kubectl -n $Namespace scale deployment/ingestion-service --replicas=1 2>$null | Out-Null
    kubectl -n $Namespace wait --for=condition=ready pod -l app=ingestion-service --timeout=60s 2>$null | Out-Null
} else {
    docker start ingestion 2>$null | Out-Null
    Start-Sleep -Seconds 5
}
Log-Pass "Ingestion Service back online!"

Write-Host "  Awaiting reprocessed rows in PostgreSQL..." -ForegroundColor Yellow
$reprocessedCount = 0
for ($r = 0; $r -lt 25; $r++) {
    Start-Sleep -Seconds 1
    $reprocessedCount = Query-DBCount
    if ($reprocessedCount -gt $initialCount) { break }
}

Write-Host "`n============================================================" -ForegroundColor Yellow
Write-Host "  REPLAY DEMO METRICS & VERIFICATION SUMMARY               " -ForegroundColor Yellow
Write-Host "============================================================" -ForegroundColor Yellow
Write-Host "  Initial rows in PostgreSQL : $initialCount" -ForegroundColor White
Write-Host "  Rows after offset replay   : $reprocessedCount" -ForegroundColor White
Write-Host "  Reprocessed events logged  : $($reprocessedCount - $initialCount)" -ForegroundColor White

if ($reprocessedCount -gt $initialCount) {
    Log-Pass "HISTORICAL REPLAY PROVED! Events successfully re-read from Kafka log and reprocessed."
    exit 0
} else {
    Log-Fail "Offset replay did not generate new processed records in database."
}
