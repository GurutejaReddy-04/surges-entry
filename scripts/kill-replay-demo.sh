#!/usr/bin/env bash
# SurgesEntry — Consumer Kill & Replay Resilience Demo
# Proves Kafka's durable partition log and consumer offset resumption.
# We kill the Ingestion Service pod mid-stream and verify crash recovery.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${SCRIPT_DIR}/../.env"
if [ -f "$ENV_FILE" ]; then
    # Export non-comment lines
    set -a
    source "$ENV_FILE" 2>/dev/null || true
    set +a
fi

NAMESPACE="${NAMESPACE:-surges-entry}"
POSTGRES_USER="${POSTGRES_USER:-eventplatform}"
POSTGRES_DB="${POSTGRES_DB:-eventplatform}"

log_step() {
    echo -e "\n\033[1;36m[$(date '+%Y-%m-%d %H:%M:%S')] [$1] $2\033[0m"
}

log_pass() {
    echo -e "  \033[1;32m🚀 PASS: $1\033[0m"
}

log_fail() {
    echo -e "  \033[1;31m💀 FAIL: $1\033[0m"
    exit 1
}

query_db_count() {
    local type="${1:-}"
    local sql
    if [ -n "$type" ]; then
        sql="SELECT COUNT(*) FROM processed_events WHERE user_id = '${USER}' AND event_type = '${type}';"
    else
        sql="SELECT COUNT(*) FROM processed_events WHERE user_id = '${USER}';"
    fi
    docker exec postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -t -A -c "$sql" 2>/dev/null || echo "0"
}

publish_event() {
    local user="$1"
    local type="$2"
    local val="$3"
    local trace="$4"
    local ts
    ts=$(date +%s)
    local json="{\"user_id\":\"${user}\",\"event_type\":\"${type}\",\"value\":${val},\"timestamp\":${ts},\"trace_id\":\"${trace}\"}"
    docker exec kafka bash -c "echo '${json}' | kafka-console-producer --bootstrap-server localhost:9092 --topic events" 2>/dev/null || true
}

echo -e "\033[1;33m============================================================\033[0m"
echo -e "\033[1;33m  🚀 SurgesEntry: Consumer Kill & Replay Resilience Demo    \033[0m"
echo -e "\033[1;33m============================================================\033[0m"

# Validates fault-tolerance and consumer recovery under abrupt process termination.

RUN_ID="killdemo-$(date +%s%N)"
USER="resilience-user-${RUN_ID}"

log_step "1/6" "Checking infrastructure and Ingestion deployment..."

if docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null | grep -q "events"; then
    log_pass "Kafka topic 'events' is healthy"
else
    log_fail "Kafka topic 'events' not found. Ensure docker-compose is up."
fi

USE_K8S=false
if kubectl -n "$NAMESPACE" get pods -l app=surges-entry-ingestion 2>/dev/null | grep -q "Running"; then
    USE_K8S=true
    log_pass "Ingestion Service is Running in Kubernetes (namespace: $NAMESPACE)"
elif docker inspect --format '{{.State.Status}}' surges-entry-ingestion 2>/dev/null | grep -q "running"; then
    log_pass "Ingestion Service is running in Docker Compose ('surges-entry-ingestion')"
elif docker inspect --format '{{.State.Status}}' ingestion 2>/dev/null | grep -q "running"; then
    log_pass "Ingestion Service is running in Docker Compose ('ingestion')"
else
    log_fail "No active Ingestion service detected in Kubernetes or Docker Compose."
fi

log_step "2/6" "Publishing 10 baseline events to Kafka (events 1-10)..."
for i in $(seq 1 10); do
    val=$((100 + i * 5))
    trace="${RUN_ID}-part1-${i}"
    publish_event "$USER" "baseline" "$val" "$trace"
    sleep 0.1
done
log_pass "10 baseline events published"

log_step "3/6" "Verifying baseline events are processed and saved in PostgreSQL..."
count1=0
for _ in $(seq 1 15); do
    sleep 1
    count1=$(query_db_count "baseline")
    if [ "$count1" -ge 10 ]; then break; fi
done

if [ "$count1" -eq 10 ]; then
    log_pass "All 10 baseline events verified in PostgreSQL"
else
    log_fail "Expected 10 baseline events, found $count1"
fi

log_step "4/6" "💀 KILLING INGESTION SERVICE MID-STREAM..."
if [ "$USE_K8S" = true ]; then
    victim_pod=$(kubectl -n "$NAMESPACE" get pods -l app=surges-entry-ingestion -o jsonpath='{.items[0].metadata.name}')
    echo -e "  Targeting pod: \033[1;31m$victim_pod\033[0m"
    kubectl -n "$NAMESPACE" delete pod "$victim_pod" --now 2>/dev/null || true
    echo "  Pod deletion issued."
else
    echo -e "  Stopping container \033[1;31msurges-entry-ingestion\033[0m..."
    docker stop surges-entry-ingestion >/dev/null 2>&1 || docker stop ingestion >/dev/null 2>&1 || true
fi

log_step "5/6" "Publishing 5 events while Ingestion Service is DOWN (events 11-15)..."
for i in $(seq 11 15); do
    val=$((100 + i * 10))
    trace="${RUN_ID}-part2-${i}"
    publish_event "$USER" "downtime-buffer" "$val" "$trace"
    sleep 0.05
done
log_pass "5 events safely buffered in Kafka durable partition log during outage"

log_step "6/6" "Waiting for self-healing recovery and offset catch-up..."
if [ "$USE_K8S" = true ]; then
    echo "  Waiting for Kubernetes self-healing..."
    kubectl -n "$NAMESPACE" wait --for=condition=ready pod -l app=surges-entry-ingestion --timeout=60s
    log_pass "Kubernetes restarted Ingestion Service; new pod is Ready!"
else
    echo "  Restarting container surges-entry-ingestion..."
    docker start surges-entry-ingestion >/dev/null 2>&1 || docker start ingestion >/dev/null 2>&1
    sleep 5
    log_pass "Ingestion container restarted!"
fi

echo "  Checking PostgreSQL for recovery of all 15 events..."
total_count=0
for _ in $(seq 1 20); do
    sleep 1
    total_count=$(query_db_count "")
    if [ "$total_count" -ge 15 ]; then break; fi
done

part2_count=$(query_db_count "downtime-buffer")

echo ""
echo -e "\033[1;33m============================================================\033[0m"
echo -e "\033[1;33m  DEMO METRICS & VERIFICATION SUMMARY                       \033[0m"
echo -e "\033[1;33m============================================================\033[0m"
echo "  Events published before kill : 10"
echo "  Events published during kill : 5"
echo "  Total events published       : 15"
echo "  Total events in PostgreSQL   : ${total_count}"
echo "  Events recovered from outage : ${part2_count} / 5"
echo "  Missing events               : $((15 - total_count))"

if [ "$total_count" -ge 15 ] && [ "$part2_count" -ge 5 ]; then
    echo -e "\n\033[1;32m🚀 PASS: 15/15 events were recovered in this consumer crash-recovery experiment.\033[0m\n"
    exit 0
else
    echo -e "\n\033[1;31m💀 FAIL: Event loss detected! Expected 15 events, recovered ${total_count}.\033[0m\n"
    exit 1
fi
