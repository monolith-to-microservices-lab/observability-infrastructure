#!/usr/bin/env bash
# Global health check across the whole migration lab. Read-only: only issues
# curl/psql SELECT queries, never mutates anything.
set -uo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
CONNECTOR_NAME="${CONNECTOR_NAME:-legacy-cdc-connector}"

ok=0
fail=0

check_http() {
  local label="$1" url="$2"
  if curl -sf -o /dev/null --max-time 3 "$url"; then
    printf " %-25s UP\n" "$label"
    ok=$((ok+1))
  else
    printf " %-25s DOWN\n" "$label"
    fail=$((fail+1))
  fi
}

check_container_healthy() {
  local label="$1" name="$2"
  local status
  status=$(docker inspect --format '{{.State.Health.Status}}' "$name" 2>/dev/null)
  if [ "$status" = "healthy" ]; then
    printf " %-25s UP\n" "$label"
    ok=$((ok+1))
  elif [ -z "$status" ]; then
    # no healthcheck defined - fall back to "is it running"
    if [ "$(docker inspect --format '{{.State.Running}}' "$name" 2>/dev/null)" = "true" ]; then
      printf " %-25s UP\n" "$label"
      ok=$((ok+1))
    else
      printf " %-25s DOWN\n" "$label"
      fail=$((fail+1))
    fi
  else
    printf " %-25s DOWN (%s)\n" "$label" "$status"
    fail=$((fail+1))
  fi
}

echo "=================================================="
echo " MIGRATION OBSERVABILITY HEALTH"
echo "=================================================="
echo

check_http "Monolith API"            "http://localhost:8000/health"
check_container_healthy "Legacy PostgreSQL" "monolito-microservice-postgres-1"
check_http "Prometheus"              "http://localhost:9090/-/healthy"
check_http "Grafana"                 "http://localhost:3000/api/health"
check_http "Loki"                    "http://localhost:3100/ready"
check_http "Tempo"                   "http://localhost:3200/ready"
check_http "OpenTelemetry Collector" "http://localhost:13133/"
check_container_healthy "Kafka"      "cdc-kafka"
check_container_healthy "Kafka Connect" "cdc-connect"

connector_state=$(curl -sf --max-time 3 "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status" | python -c "import json,sys; print(json.load(sys.stdin)['connector']['state'])" 2>/dev/null)
if [ "$connector_state" = "RUNNING" ]; then
  printf " %-25s RUNNING\n" "Debezium Connector"
  ok=$((ok+1))
else
  printf " %-25s %s\n" "Debezium Connector" "${connector_state:-UNREACHABLE}"
  fail=$((fail+1))
fi

slot_active=$(curl -sf --max-time 3 "http://localhost:9187/metrics" 2>/dev/null | grep '^pg_replication_slot_active{' | grep 'legacy_cdc_slot' | awk '{print $2}')
if [ "$slot_active" = "1" ]; then
  printf " %-25s ACTIVE\n" "Replication Slot"
  ok=$((ok+1))
else
  printf " %-25s INACTIVE/UNKNOWN\n" "Replication Slot"
  fail=$((fail+1))
fi

check_http "User CDC Consumer"       "http://localhost:9200/metrics"
check_container_healthy "User PostgreSQL" "user-service-user-postgres-1"
check_http "Sales CDC Consumer"      "http://localhost:9201/metrics"
check_container_healthy "Sales PostgreSQL" "sales-service-sales-postgres-1"

echo
user_lag=$(curl -sf --max-time 3 "http://localhost:9308/metrics" 2>/dev/null | grep '^kafka_consumergroup_lag{' | grep 'user-service-cdc' | awk '{sum+=$2} END {print sum+0}')
printf " %-25s %s\n" "User Consumer Lag" "${user_lag:-unknown}"

sales_lag=$(curl -sf --max-time 3 "http://localhost:9308/metrics" 2>/dev/null | grep '^kafka_consumergroup_lag{' | grep 'sales-service-cdc' | awk '{sum+=$2} END {print sum+0}')
printf " %-25s %s\n" "Sales Consumer Lag" "${sales_lag:-unknown}"

user_last_ts=$(curl -sf --max-time 3 "http://localhost:9200/metrics" 2>/dev/null | grep '^cdc_last_event_timestamp_seconds' | awk '{print $2}')
if [ -n "$user_last_ts" ]; then
  age=$(python -c "import time,sys; print(int(time.time() - float(sys.argv[1])))" "$user_last_ts" 2>/dev/null)
  printf " %-25s %ss ago\n" "User CDC Last Event" "${age:-unknown}"
else
  printf " %-25s unknown\n" "User CDC Last Event"
fi

sales_last_ts=$(curl -sf --max-time 3 "http://localhost:9201/metrics" 2>/dev/null | grep '^cdc_last_event_timestamp_seconds' | awk '{print $2}')
if [ -n "$sales_last_ts" ]; then
  age=$(python -c "import time,sys; print(int(time.time() - float(sys.argv[1])))" "$sales_last_ts" 2>/dev/null)
  printf " %-25s %ss ago\n" "Sales CDC Last Event" "${age:-unknown}"
else
  printf " %-25s unknown\n" "Sales CDC Last Event"
fi

echo
echo "=================================================="
if [ "$fail" -eq 0 ]; then
  echo " OVERALL                   HEALTHY"
else
  echo " OVERALL                   DEGRADED ($fail check(s) failed)"
fi
echo "=================================================="
