#!/usr/bin/env pwsh
# Phase 8 Verification: OpenTelemetry distributed tracing & Jaeger validation
$ErrorActionPreference = "Continue"
$failed = 0

function Step([string]$n, [string]$label) {
    Write-Host "`n--- [$n] $label ---" -ForegroundColor Cyan
}
function Pass([string]$msg) { Write-Host "  PASS  $msg" -ForegroundColor Green }
function Fail([string]$msg) { $script:failed++; Write-Host "  FAIL  $msg" -ForegroundColor Red }

# 1. Container health
Step "1/5" "Verify container health"
$services = @("kafka", "redis", "postgres", "jaeger", "notification", "processing", "ingestion")
foreach ($s in $services) {
    $status = docker inspect --format '{{.State.Health.Status}}' $s 2>$null
    if ($status -eq "healthy") {
        Pass "$s is healthy"
    } else {
        Fail "$s status: $status (expected: healthy)"
    }
}

# 2. Jaeger API service registration
Step "2/5" "Verify Jaeger registered services"
try {
    $resp = Invoke-RestMethod -Uri "http://localhost:16686/api/services" -Method Get
    $registered = $resp.data
    $required = @("ingestion-service", "processing-service", "notification-service")
    foreach ($svc in $required) {
        if ($registered -contains $svc) {
            Pass "Jaeger registered service: $svc"
        } else {
            Fail "Jaeger missing service: $svc"
        }
    }
} catch {
    Fail "Failed to query Jaeger API: $_"
}

# 3. Distributed Trace spanning all 3 microservices
Step "3/5" "Verify multi-hop distributed trace in Jaeger"
$traceFound = $false
$sampledTraceId = ""
try {
    $traceResp = Invoke-RestMethod -Uri "http://localhost:16686/api/traces?service=notification-service&limit=5" -Method Get
    if ($traceResp.data.Count -gt 0) {
        $trace = $traceResp.data[0]
        $sampledTraceId = $trace.traceID
        $spanOperations = $trace.spans | ForEach-Object { $_.operationName }
        $spanServices = $trace.processes.PSObject.Properties | ForEach-Object { $_.Value.serviceName } | Select-Object -Unique

        Pass "Found distributed trace ID: $sampledTraceId"
        Write-Host "    Spans recorded: $($trace.spans.Count)" -ForegroundColor Gray
        foreach ($op in ($spanOperations | Select-Object -Unique)) {
            Write-Host "    - Operation: $op" -ForegroundColor DarkGray
        }

        if ($spanServices -contains "ingestion-service" -and $spanServices -contains "processing-service" -and $spanServices -contains "notification-service") {
            Pass "Trace traverses all 3 microservices (ingestion -> processing -> notification)"
            $traceFound = $true
        } else {
            Fail "Trace does not span all 3 services. Present: $($spanServices -join ', ')"
        }
    } else {
        Fail "No traces found for notification-service in Jaeger"
    }
} catch {
    Fail "Failed to inspect Jaeger traces: $_"
}

# 4. PostgreSQL Trace ID persistence
Step "4/5" "Verify trace_id persistence in PostgreSQL"
if ($sampledTraceId) {
    $pgResult = docker exec postgres psql -U eventplatform -d eventplatform -t -A -c "SELECT user_id, event_type, event_value, rolling_avg, is_anomaly FROM processed_events WHERE trace_id = '$sampledTraceId';" 2>$null
    if ($pgResult) {
        Pass "Database row found with trace_id $sampledTraceId"
        Write-Host "    DB Record: $pgResult" -ForegroundColor Gray
    } else {
        Fail "Trace ID $sampledTraceId not found in processed_events table"
    }
} else {
    Fail "Skipping DB check due to missing sample trace ID"
}

# 5. Summary
Write-Host "`n=== Phase 8 Verification Summary ===" -ForegroundColor Cyan
if ($failed -eq 0) {
    Write-Host "ALL CHECKS PASSED: OpenTelemetry distributed tracing and Jaeger integration fully verified!" -ForegroundColor Green
    exit 0
} else {
    Write-Host "$failed CHECK(S) FAILED." -ForegroundColor Red
    exit 1
}
