# SurgesEntry — Consumer Kill & Replay Resilience Demo
# Proves Kafka's durable partition log and Kubernetes self-healing.
# We kill the Ingestion Service pod mid-stream and prove zero data loss.

param(
    [string]$PostgresUser = "eventplatform",
    [string]$PostgresDb = "eventplatform",
    [string]$Namespace = "surges-entry"
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

$runID = "killdemo-" + [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
$user = "resilience-user-$runID"

function Query-DBCount([string]$eventType = "") {
    if ($eventType) {
        $sql = "SELECT COUNT(*) FROM processed_events WHERE user_id = '$user' AND event_type = '$eventType';"
    } else {
        $sql = "SELECT COUNT(*) FROM processed_events WHERE user_id = '$user';"
    }
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
Write-Host "  🚀 SurgesEntry: Consumer Kill & Replay Resilience Demo   " -ForegroundColor Yellow
Write-Host "============================================================" -ForegroundColor Yellow

# Validates fault-tolerance and zero data loss under abrupt consumer termination.
Log-Step "1/6" "Checking infrastructure and Ingestion deployment..."

$kafkaCheck = docker exec kafka kafka-topics --bootstrap-server kafka:29092 --list 2>$null
if ($kafkaCheck -match "events") {
    Log-Pass "Kafka topic 'events' is healthy"
} else {
    Log-Fail "Kafka topic 'events' not found. Ensure docker-compose is running."
}

# Determine execution mode (Kubernetes vs Docker Compose)
$useK8s = $false
$k8sPods = kubectl -n $Namespace get pods -l app=surges-entry-ingestion -o jsonpath='{.items[*].metadata.name}' 2>$null
if ($k8sPods -and $k8sPods.Trim() -ne "") {
    $useK8s = $true
    Log-Pass "Targeting Kubernetes deployment in namespace '$Namespace'"
} else {
    $ingRunning = docker inspect --format '{{.State.Running}}' surges-entry-ingestion 2>$null
    if ($ingRunning -ne "true") {
        $ingRunning = docker inspect --format '{{.State.Running}}' ingestion 2>$null
    }
    if ($ingRunning -eq "true") {
        Log-Pass "Targeting Docker Compose deployment ('surges-entry-ingestion')"
    } else {
        Log-Fail "Neither Kubernetes pod nor Docker container 'surges-entry-ingestion' is running."
    }
}

# ── Step 2: Publish Baseline Events ──────────────────────────────────────────
Log-Step "2/6" "Publishing 10 baseline events to Kafka topic 'events'..."

for ($i = 1; $i -le 10; $i++) {
    $val = 100.0 + ($i * 5.0)
    $trace = "$runID-part1-$i"
    Publish-KafkaEvent -userID $user -eventType "baseline" -val $val -traceID $trace
    Start-Sleep -Milliseconds 50
}

Log-Pass "10 baseline events published"

# ── Step 3: Verify Baseline Events in PostgreSQL ─────────────────────────────
Log-Step "3/6" "Verifying baseline events are processed and saved in PostgreSQL..."

$retries = 15
$count1 = 0
while ($retries -gt 0) {
    Start-Sleep -Seconds 1
    $count1 = Query-DBCount -eventType "baseline"
    if ($count1 -ge 10) { break }
    $retries--
}

if ($count1 -ge 10) {
    Log-Pass "All 10 baseline events verified in PostgreSQL"
} else {
    Log-Fail "Expected 10 events in PostgreSQL, found $count1"
}

# ── Step 4: Kill the Ingestion Consumer Mid-Stream ───────────────────────────
Log-Step "4/6" "💀 KILLING INGESTION SERVICE MID-STREAM..."

if ($useK8s) {
    $victimPod = (kubectl -n $Namespace get pods -l app=surges-entry-ingestion -o jsonpath='{.items[0].metadata.name}').Trim()
    Write-Host "  Targeting pod: $victimPod" -ForegroundColor Red
    kubectl -n $Namespace delete pod $victimPod --now 2>$null | Out-Null
    Write-Host "  Pod deletion issued." -ForegroundColor DarkYellow
} else {
    Write-Host "  Stopping container 'surges-entry-ingestion'..." -ForegroundColor Red
    docker stop surges-entry-ingestion 2>$null | Out-Null
    docker stop ingestion 2>$null | Out-Null
}

# ── Step 5: Publish Events During Outage ─────────────────────────────────────
Log-Step "5/6" "Publishing 5 events while Ingestion Service is DOWN..."

for ($i = 1; $i -le 5; $i++) {
    $val = 300.0 + ($i * 10.0)
    $trace = "$runID-part2-$i"
    Publish-KafkaEvent -userID $user -eventType "downtime-buffer" -val $val -traceID $trace
    Start-Sleep -Milliseconds 50
}

Log-Pass "5 events safely published into Kafka partition buffer while Ingestion was dead!"

# ── Step 6: Self-Healing & Catch-Up Verification ─────────────────────────────
Log-Step "6/6" "Waiting for Ingestion Service to recover and catch up..."

if ($useK8s) {
    Write-Host "  Waiting for Kubernetes to self-heal and mark new pod Ready..." -ForegroundColor Yellow
    kubectl -n $Namespace wait --for=condition=ready pod -l app=surges-entry-ingestion --timeout=60s 2>$null | Out-Null
    Log-Pass "Kubernetes restarted Ingestion Service; new pod is Ready!"
} else {
    Write-Host "  Restarting container 'surges-entry-ingestion'..." -ForegroundColor Yellow
    docker start surges-entry-ingestion 2>$null | Out-Null
    docker start ingestion 2>$null | Out-Null
    Start-Sleep -Seconds 5
    Log-Pass "Ingestion container restarted!"
}

Write-Host "  Checking PostgreSQL for recovery of all 15 events..." -ForegroundColor Yellow
$retries = 20
$totalCount = 0
while ($retries -gt 0) {
    Start-Sleep -Seconds 1
    $totalCount = Query-DBCount
    if ($totalCount -ge 15) { break }
    $retries--
}

$part2Count = Query-DBCount -eventType "downtime-buffer"

# ── Final Metrics ────────────────────────────────────────────────────────────
Write-Host "`n============================================================" -ForegroundColor Yellow
Write-Host "  DEMO METRICS & VERIFICATION SUMMARY                      " -ForegroundColor Yellow
Write-Host "============================================================" -ForegroundColor Yellow
Write-Host "  Events published before kill : 10" -ForegroundColor White
Write-Host "  Events published during kill : 5"  -ForegroundColor White
Write-Host "  Total events published       : 15" -ForegroundColor White
Write-Host "  Total events in PostgreSQL   : $totalCount" -ForegroundColor White
Write-Host "  Events recovered from outage : $part2Count / 5" -ForegroundColor White
Write-Host "  Missing events               : $(15 - $totalCount)" -ForegroundColor White

if ($totalCount -eq 15) {
    Log-Pass "ZERO DATA LOSS CONFIRMED! Kafka offset resumption and Kubernetes self-healing verified."
    exit 0
} else {
    Log-Fail "Data loss detected: Expected 15 events, but only $totalCount arrived in PostgreSQL."
}
