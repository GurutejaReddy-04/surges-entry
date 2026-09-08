#!/usr/bin/env bash
# Phase 7 — Kafka Consumer Group Offset Replay Demo
# Proves Kafka's capability to reprocess historical event streams on demand.

set -euo pipefail

NAMESPACE="${NAMESPACE:-event-platform}"
CONSUMER_GROUP="ingestion-service-group"
POSTGRES_USER="eventplatform"
POSTGRES_DB="eventplatform"

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
    local sql="SELECT COUNT(*) FROM processed_events WHERE user_id = '${USER}';"
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
echo -e "\033[1;33m  🚀 PHASE 7: KAFKA OFFSET REPLAY DEMO                      \033[0m"
echo -e "\033[1;33m============================================================\033[0m"

RUN_ID="replaydemo-$(date +%s%N)"
USER="replay-user-${RUN_ID}"

log_step "1/5" "Checking deployment environment..."
USE_K8S=false
if kubectl -n "$NAMESPACE" get pods -l app=ingestion-service 2>/dev/null | grep -q "Running"; then
    USE_K8S=true
    CONSUMER_GROUP="k8s-ingestion-group"
    log_pass "Targeting Kubernetes consumer group '$CONSUMER_GROUP'"
else
    log_pass "Targeting Docker Compose consumer group '$CONSUMER_GROUP'"
fi

log_step "2/5" "Publishing 5 distinct events for replay tracking..."
for i in $(seq 1 5); do
    val=$((50 + i * 10))
    trace="${RUN_ID}-event-${i}"
    publish_event "$USER" "replay-test" "$val" "$trace"
    sleep 0.1
done
log_pass "5 events published"

initial_count=0
for _ in $(seq 1 15); do
    sleep 1
    initial_count=$(query_db_count)
    if [ "$initial_count" -ge 5 ]; then break; fi
done

if [ "$initial_count" -ge 5 ]; then
    log_pass "Initial batch processed into PostgreSQL (count: $initial_count)"
else
    log_fail "Initial batch failed to process (only $initial_count in DB)"
fi

log_step "3/5" "Pausing Ingestion Service (Kafka requires inactive consumer group to alter offsets)..."
if [ "$USE_K8S" = true ]; then
    kubectl -n "$NAMESPACE" scale deployment/ingestion-service --replicas=0 >/dev/null 2>&1
    sleep 3
else
    docker stop ingestion >/dev/null 2>&1
    sleep 2
fi
log_pass "Consumer group is now inactive and ready for offset manipulation"

log_step "4/5" "Rewinding Kafka offsets to --to-earliest for topic 'events'..."
docker exec kafka kafka-consumer-groups \
    --bootstrap-server localhost:9092 \
    --group "$CONSUMER_GROUP" \
    --topic events \
    --reset-offsets \
    --to-earliest \
    --execute

log_pass "Consumer group offsets rewound to earliest position"

log_step "5/5" "Restarting Ingestion Service and observing historical replay..."
if [ "$USE_K8S" = true ]; then
    kubectl -n "$NAMESPACE" scale deployment/ingestion-service --replicas=1 >/dev/null 2>&1
    kubectl -n "$NAMESPACE" wait --for=condition=ready pod -l app=ingestion-service --timeout=60s
else
    docker start ingestion >/dev/null 2>&1
    sleep 5
fi
log_pass "Ingestion Service back online!"

echo "  Awaiting reprocessed rows in PostgreSQL..."
reprocessed_count=0
for _ in $(seq 1 20); do
    sleep 1
    reprocessed_count=$(query_db_count)
    if [ "$reprocessed_count" -gt "$initial_count" ]; then break; fi
done

echo -e "\n\033[1;33m============================================================\033[0m"
echo -e "\033[1;33m  REPLAY DEMO METRICS & VERIFICATION SUMMARY                \033[0m"
echo -e "\033[1;33m============================================================\033[0m"
echo "  Initial rows in PostgreSQL : $initial_count"
echo "  Rows after offset replay   : $reprocessed_count"
echo "  Reprocessed events logged  : $((reprocessed_count - initial_count))"

if [ "$reprocessed_count" -gt "$initial_count" ]; then
    log_pass "HISTORICAL REPLAY PROVED! Events successfully re-read from Kafka log and reprocessed."
    exit 0
else
    log_fail "Offset replay did not generate new processed records in database."
fi
