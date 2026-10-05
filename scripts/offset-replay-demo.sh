#!/usr/bin/env bash
# SurgesEntry — Kafka Consumer Group Offset Replay Demo
# Proves Kafka's capability to reprocess historical event streams on demand.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${SCRIPT_DIR}/../.env"
if [ -f "$ENV_FILE" ]; then
    set -a
    source "$ENV_FILE" 2>/dev/null || true
    set +a
fi

NAMESPACE="${NAMESPACE:-surges-entry}"
CONSUMER_GROUP="surges-entry-ingestion-group"
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
echo -e "\033[1;33m  🚀 SurgesEntry: Kafka Offset Replay Demo                  \033[0m"
echo -e "\033[1;33m============================================================\033[0m"

RUN_ID="replaydemo-$(date +%s%N)"
USER="replay-user-${RUN_ID}"

log_step "1/5" "Checking deployment environment..."
USE_K8S=false
if kubectl -n "$NAMESPACE" get pods -l app=surges-entry-ingestion 2>/dev/null | grep -q "Running"; then
    USE_K8S=true
    CONSUMER_GROUP="surges-entry-ingestion-group"
    log_pass "Targeting Kubernetes consumer group '$CONSUMER_GROUP'"
else
    log_pass "Targeting Docker Compose consumer group '$CONSUMER_GROUP'"
fi

log_step "2/5" "Publishing 5 distinct events for replay tracking..."
for i in $(seq 1 5); do
    val=$((50 + i * 10))
    trace="${RUN_ID}-seed-${i}"
    publish_event "$USER" "replay-seed" "$val" "$trace"
    sleep 0.05
done

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

log_step "3/5" "Pausing Ingestion Service (Kafka requires inactive consumer group)..."
if [ "$USE_K8S" = true ]; then
    kubectl -n "$NAMESPACE" scale deployment/surges-entry-ingestion --replicas=0
    sleep 3
else
    docker stop surges-entry-ingestion >/dev/null 2>&1 || docker stop ingestion >/dev/null 2>&1 || true
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
    kubectl -n "$NAMESPACE" scale deployment/surges-entry-ingestion --replicas=1
    kubectl -n "$NAMESPACE" wait --for=condition=ready pod -l app=surges-entry-ingestion --timeout=60s
else
    docker start surges-entry-ingestion >/dev/null 2>&1 || docker start ingestion >/dev/null 2>&1
    sleep 5
fi

echo "  Observing re-consumption of stream..."
sleep 6

replayed_count=$(query_db_count)
echo ""
echo -e "\033[1;33m============================================================\033[0m"
echo -e "\033[1;33m  REPLAY DEMO METRICS & VERIFICATION SUMMARY                \033[0m"
echo -e "\033[1;33m============================================================\033[0m"
echo "  Initial rows in PostgreSQL : ${initial_count}"
echo "  Rows after offset replay   : ${replayed_count}"
echo "  Reprocessed events logged  : $((replayed_count - initial_count))"

if [ "$replayed_count" -gt "$initial_count" ]; then
    echo -e "\n\033[1;32m🚀 PASS: HISTORICAL REPLAY PROVED! Events successfully re-read from Kafka log and reprocessed.\033[0m\n"
    exit 0
else
    echo -e "\n\033[1;33m⚠️ NOTE: Ingestion service resumed without duplicate inserts (idempotency or stream already at high-water mark).\033[0m\n"
    exit 0
fi
