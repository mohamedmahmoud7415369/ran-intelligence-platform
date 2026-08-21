#!/usr/bin/env bash
set -Eeuo pipefail

STACK_NAME="${STACK_NAME:-ran-platform}"
TOPIC="${DEMO_TOPIC:-ran-4g-demo}"
CONNECTOR="${DEMO_CONNECTOR:-ran-4g-hdfs-sink}"
SOURCE_FILE="${SOURCE_FILE:-Dataset_01_LTE_2100.csv}"
HDFS_TOPIC_PATH="${HDFS_TOPIC_PATH:-/ran-4g-demo-data/${TOPIC}}"
REQUIRE_FULL_FILE="${REQUIRE_FULL_FILE:-1}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
SOURCE_PATH="${PROJECT_ROOT}/data/Raw/${SOURCE_FILE}"

FAILURES=0

ok() {
  echo "[PASS] $*"
}

fail() {
  echo "[FAIL] $*" >&2
  FAILURES=$((FAILURES + 1))
}

service_container() {
  local service_name="$1"
  docker ps -q \
    --filter "label=com.docker.swarm.service.name=${STACK_NAME}_${service_name}" \
    | head -n1
}

service_replicas() {
  local full_name="${STACK_NAME}_$1"
  docker service ls --format '{{.Name}} {{.Replicas}}' \
    | awk -v name="$full_name" '$1 == name {print $2}'
}

echo "============================================================"
echo "RAN 4G demo validation"
echo "============================================================"

BAD_NODES="$(docker node ls --format '{{.Hostname}} {{.Status}} {{.Availability}}' \
  | awk '$2 != "Ready" || $3 != "Active"')"

if [[ -z "$BAD_NODES" ]]; then
  ok "All Swarm nodes are Ready and Active."
else
  fail "Some Swarm nodes are not Ready/Active:"
  echo "$BAD_NODES"
fi

CORE_SERVICES=(
  zookeeper1 zookeeper2 zookeeper3
  kafka1 kafka2 kafka3
  connect1 connect2
  journalnode1 journalnode2 journalnode3
  namenode1 namenode2
  zkfc1 zkfc2
  datanode1 datanode2 datanode3
)

for service_name in "${CORE_SERVICES[@]}"; do
  replicas="$(service_replicas "$service_name")"
  if [[ "$replicas" == "1/1" ]]; then
    ok "${service_name}=${replicas}"
  else
    fail "${service_name}=${replicas:-missing}; expected 1/1"
  fi
done

KAFKA_CONTAINER="$(service_container kafka2)"
CONNECT_CONTAINER="$(service_container connect2)"
HDFS_CLIENT="$(service_container datanode2)"

if [[ -z "$KAFKA_CONTAINER" ]]; then
  fail "No local Kafka2 container found."
else
  BROKERS="$(docker exec "$KAFKA_CONTAINER" \
    /opt/kafka/bin/kafka-broker-api-versions.sh \
    --bootstrap-server kafka1:9092,kafka2:9092,kafka3:9092 2>/dev/null \
    | grep -E '^kafka[123]:9092' || true)"

  BROKER_COUNT="$(printf '%s\n' "$BROKERS" | sed '/^$/d' | wc -l)"
  if [[ "$BROKER_COUNT" == "3" ]]; then
    ok "All three Kafka brokers respond."
  else
    fail "Only ${BROKER_COUNT}/3 Kafka brokers respond."
  fi

  if docker exec "$KAFKA_CONTAINER" \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka2:9092 \
    --list | grep -qx "$TOPIC"; then

    KAFKA_RECORDS="$(docker exec "$KAFKA_CONTAINER" \
      /opt/kafka/bin/kafka-get-offsets.sh \
      --bootstrap-server kafka2:9092 \
      --topic "$TOPIC" 2>/dev/null \
      | awk -F: '{sum += $3} END {print sum+0}')"
    ok "Kafka topic ${TOPIC} contains ${KAFKA_RECORDS} records."
  else
    KAFKA_RECORDS=0
    fail "Kafka topic ${TOPIC} does not exist."
  fi
fi

if [[ -f "$SOURCE_PATH" ]]; then
  EXPECTED_RECORDS="$(( $(wc -l < "$SOURCE_PATH") - 1 ))"
  ok "CSV ${SOURCE_FILE} contains ${EXPECTED_RECORDS} data rows."

  if [[ "$REQUIRE_FULL_FILE" == "0" ]]; then
    ok "Full CSV count check skipped (REQUIRE_FULL_FILE=0)."
  elif [[ "${KAFKA_RECORDS:-0}" == "$EXPECTED_RECORDS" ]]; then
    ok "CSV and Kafka record counts match."
  else
    fail "CSV=${EXPECTED_RECORDS}, Kafka=${KAFKA_RECORDS:-0}."
  fi
else
  EXPECTED_RECORDS=0
  fail "CSV file not found: ${SOURCE_PATH}"
fi

if [[ -z "$CONNECT_CONTAINER" ]]; then
  fail "No local Connect2 container found."
else
  CONNECT_STATUS="$(docker exec "$CONNECT_CONTAINER" \
    curl -fsS --max-time 20 \
    "http://localhost:8083/connectors/${CONNECTOR}/status" 2>/dev/null || true)"

  CONNECT_STATE="$(python3 -c \
    'import json,sys; d=json.load(sys.stdin); print(d["connector"]["state"])' \
    <<<"$CONNECT_STATUS" 2>/dev/null || true)"

  BAD_TASKS="$(python3 -c \
    'import json,sys; d=json.load(sys.stdin); print(sum(t["state"] != "RUNNING" for t in d["tasks"]))' \
    <<<"$CONNECT_STATUS" 2>/dev/null || true)"

  if [[ "$CONNECT_STATE" == "RUNNING" && "$BAD_TASKS" == "0" ]]; then
    ok "Connector and all connector tasks are RUNNING."
  else
    fail "Connector state=${CONNECT_STATE:-unknown}, bad tasks=${BAD_TASKS:-unknown}."
  fi
fi

if [[ -n "$KAFKA_CONTAINER" ]]; then
  CONNECT_LAG="$(docker exec "$KAFKA_CONTAINER" \
    /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server kafka2:9092 \
    --group "connect-${CONNECTOR}" \
    --describe 2>/dev/null \
    | awk -v topic="$TOPIC" '$2 == topic {sum += $6; found=1} END {if (found) print sum; else print -1}')"

  if [[ "$CONNECT_LAG" == "0" ]]; then
    ok "Kafka Connect lag is zero."
  else
    fail "Kafka Connect lag is ${CONNECT_LAG}."
  fi
fi

if [[ -z "$HDFS_CLIENT" ]]; then
  fail "No local DataNode2 container found for HDFS checks."
else
  HA_STATES="$(docker exec "$HDFS_CLIENT" \
    hdfs haadmin -getAllServiceState 2>/dev/null || true)"
  ACTIVE_COUNT="$(printf '%s\n' "$HA_STATES" | awk '$2 == "active" {count++} END {print count+0}')"
  STANDBY_COUNT="$(printf '%s\n' "$HA_STATES" | awk '$2 == "standby" {count++} END {print count+0}')"

  if [[ "$ACTIVE_COUNT" == "1" && "$STANDBY_COUNT" == "1" ]]; then
    ok "HDFS HA has one Active and one Standby NameNode."
    echo "$HA_STATES"
  else
    fail "Invalid HDFS HA states:"
    echo "$HA_STATES"
  fi

  LIVE_DATANODES="$(docker exec "$HDFS_CLIENT" hdfs dfsadmin -report 2>/dev/null \
    | sed -n 's/^Live datanodes (\([0-9][0-9]*\)):.*/\1/p' \
    | head -n1)"

  if [[ "$LIVE_DATANODES" == "3" ]]; then
    ok "All three HDFS DataNodes are live."
  else
    fail "Live HDFS DataNodes=${LIVE_DATANODES:-0}; expected 3."
  fi

  if docker exec "$HDFS_CLIENT" hdfs dfs -test -e "$HDFS_TOPIC_PATH"; then
    HDFS_RECORDS="$(docker exec "$HDFS_CLIENT" bash -lc \
      "hdfs dfs -cat '${HDFS_TOPIC_PATH}/partition=*/*.json' 2>/dev/null | wc -l")"
    HDFS_FILES="$(docker exec "$HDFS_CLIENT" hdfs dfs -find "$HDFS_TOPIC_PATH" \
      -name '*.json' | wc -l)"

    ok "HDFS contains ${HDFS_RECORDS} records in ${HDFS_FILES} JSON files."

    if [[ "$HDFS_RECORDS" == "${KAFKA_RECORDS:-0}" ]]; then
      ok "Kafka and HDFS record counts match."
    else
      fail "Kafka=${KAFKA_RECORDS:-0}, HDFS=${HDFS_RECORDS}."
    fi

    FSCK_OUTPUT="$(docker exec "$HDFS_CLIENT" \
      hdfs fsck "$HDFS_TOPIC_PATH" -files -blocks -locations 2>&1 || true)"

    if grep -q "is HEALTHY" <<<"$FSCK_OUTPUT" \
      && grep -q "Under-replicated blocks:[[:space:]]*0" <<<"$FSCK_OUTPUT" \
      && grep -q "Missing blocks:[[:space:]]*0" <<<"$FSCK_OUTPUT"; then
      ok "HDFS path is HEALTHY with no missing or under-replicated blocks."
    else
      fail "HDFS fsck validation failed."
    fi
  else
    HDFS_RECORDS=0
    fail "HDFS path does not exist: ${HDFS_TOPIC_PATH}"
  fi
fi

echo "============================================================"
if (( FAILURES == 0 )); then
  echo "VALIDATION PASSED"
  exit 0
else
  echo "VALIDATION FAILED: ${FAILURES} check(s) failed"
  exit 1
fi
