#!/usr/bin/env bash
set -Eeuo pipefail

STACK_NAME="${STACK_NAME:-ran-platform}"
KAFKA_TEST_TOPIC="${KAFKA_TEST_TOPIC:-ran-failover-test}"
KEEP_TEST_DATA="${KEEP_TEST_DATA:-0}"

KAFKA_WAS_SCALED=0
HDFS_SCALED_SERVICE=""
TEST_TOPIC_CREATED=0
HDFS_TEST_PATH=""

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

wait_replicas() {
  local service_name="$1"
  local expected="$2"
  local attempts="${3:-36}"

  for _ in $(seq 1 "$attempts"); do
    [[ "$(service_replicas "$service_name")" == "$expected" ]] && return 0
    sleep 5
  done
  return 1
}

restore_cluster() {
  local exit_code=$?
  set +e

  if [[ -n "$HDFS_SCALED_SERVICE" ]]; then
    echo "Restoring ${HDFS_SCALED_SERVICE}..."
    docker service scale \
      "${STACK_NAME}_${HDFS_SCALED_SERVICE}=1" >/dev/null 2>&1
  fi

  if (( KAFKA_WAS_SCALED == 1 )); then
    echo "Restoring kafka1..."
    docker service scale "${STACK_NAME}_kafka1=1" >/dev/null 2>&1
  fi

  if (( TEST_TOPIC_CREATED == 1 )) && [[ "$KEEP_TEST_DATA" != "1" ]]; then
    KAFKA_CLIENT="$(service_container kafka2)"
    if [[ -n "$KAFKA_CLIENT" ]]; then
      docker exec "$KAFKA_CLIENT" \
        /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server kafka2:9092 \
        --delete --topic "$KAFKA_TEST_TOPIC" \
        >/dev/null 2>&1
    fi
  fi

  if [[ -n "$HDFS_TEST_PATH" && "$KEEP_TEST_DATA" != "1" ]]; then
    HDFS_CLIENT="$(service_container datanode2)"
    if [[ -n "$HDFS_CLIENT" ]]; then
      docker exec "$HDFS_CLIENT" \
        hdfs dfs -rm -r -skipTrash "$HDFS_TEST_PATH" \
        >/dev/null 2>&1
    fi
  fi

  return "$exit_code"
}

trap restore_cluster EXIT
trap 'exit 130' INT TERM

fail() {
  echo "ERROR: $*" >&2
  exit 1
}

echo "This test temporarily stops kafka1 and the current Active NameNode."
echo "Both services are restored automatically, including on failure."
echo

if [[ "${1:-}" != "--yes" ]]; then
  read -r -p "Type FAILOVER-${STACK_NAME} to continue: " confirmation
  [[ "$confirmation" == "FAILOVER-${STACK_NAME}" ]] || fail "Cancelled."
fi

for required_service in kafka1 kafka2 kafka3 namenode1 namenode2 zkfc1 zkfc2 datanode2; do
  [[ "$(service_replicas "$required_service")" == "1/1" ]] \
    || fail "${required_service} must be 1/1 before the test."
done

KAFKA_CLIENT="$(service_container kafka2)"
HDFS_CLIENT="$(service_container datanode2)"

[[ -n "$KAFKA_CLIENT" ]] || fail "Kafka2 container is not local/running."
[[ -n "$HDFS_CLIENT" ]] || fail "DataNode2 container is not local/running."

echo
echo "================ KAFKA FAILOVER ================"

if docker exec "$KAFKA_CLIENT" \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka2:9092 --list \
  | grep -qx "$KAFKA_TEST_TOPIC"; then
  docker exec "$KAFKA_CLIENT" \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka2:9092 \
    --delete --topic "$KAFKA_TEST_TOPIC"
  sleep 5
fi

docker exec "$KAFKA_CLIENT" \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka2:9092 \
  --create \
  --topic "$KAFKA_TEST_TOPIC" \
  --partitions 3 \
  --replication-factor 3 \
  --config min.insync.replicas=2
TEST_TOPIC_CREATED=1

BEFORE_MESSAGE="before-kafka-failover-$(date -u +%Y%m%dT%H%M%SZ)"
printf '%s\n' "$BEFORE_MESSAGE" | docker exec -i "$KAFKA_CLIENT" \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka2:9092,kafka3:9092 \
  --topic "$KAFKA_TEST_TOPIC" \
  --producer-property acks=all

echo "Stopping kafka1..."
KAFKA_WAS_SCALED=1
docker service scale "${STACK_NAME}_kafka1=0"
wait_replicas kafka1 0/0 24 || fail "kafka1 did not stop."

docker exec "$KAFKA_CLIENT" \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka2:9092,kafka3:9092 \
  --describe --topic "$KAFKA_TEST_TOPIC"

DURING_MESSAGE="during-kafka1-failure-$(date -u +%Y%m%dT%H%M%SZ)"
printf '%s\n' "$DURING_MESSAGE" | docker exec -i "$KAFKA_CLIENT" \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka2:9092,kafka3:9092 \
  --topic "$KAFKA_TEST_TOPIC" \
  --producer-property acks=all

CONSUMED="$(docker exec "$KAFKA_CLIENT" \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka2:9092,kafka3:9092 \
  --topic "$KAFKA_TEST_TOPIC" \
  --from-beginning \
  --max-messages 2 \
  --timeout-ms 30000 2>/dev/null)"

grep -Fq "$BEFORE_MESSAGE" <<<"$CONSUMED" \
  || fail "Message written before Kafka failover was not consumed."
grep -Fq "$DURING_MESSAGE" <<<"$CONSUMED" \
  || fail "Message written during Kafka failover was not consumed."

echo "Kafka remained writable and readable with kafka1 stopped."

echo "Restoring kafka1..."
docker service scale "${STACK_NAME}_kafka1=1"
wait_replicas kafka1 1/1 48 || fail "kafka1 did not return to 1/1."
KAFKA_WAS_SCALED=0

for _ in $(seq 1 36); do
  if docker exec "$KAFKA_CLIENT" \
    /opt/kafka/bin/kafka-broker-api-versions.sh \
    --bootstrap-server kafka1:9092 2>/dev/null \
    | grep -q '^kafka1:9092'; then
    break
  fi
  sleep 5
done

docker exec "$KAFKA_CLIENT" \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka2:9092 \
  --describe --topic "$KAFKA_TEST_TOPIC"

echo "Kafka failover test PASSED."

echo
echo "================ HDFS FAILOVER ================="

NN1_STATE="$(docker exec "$HDFS_CLIENT" \
  hdfs haadmin -getServiceState nn1 2>/dev/null || true)"
NN2_STATE="$(docker exec "$HDFS_CLIENT" \
  hdfs haadmin -getServiceState nn2 2>/dev/null || true)"

if [[ "$NN1_STATE" == "active" && "$NN2_STATE" == "standby" ]]; then
  ACTIVE_ID=nn1
  NEW_ACTIVE_ID=nn2
  ACTIVE_SERVICE=namenode1
elif [[ "$NN2_STATE" == "active" && "$NN1_STATE" == "standby" ]]; then
  ACTIVE_ID=nn2
  NEW_ACTIVE_ID=nn1
  ACTIVE_SERVICE=namenode2
else
  fail "Expected one Active and one Standby NameNode; nn1=${NN1_STATE}, nn2=${NN2_STATE}."
fi

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
HDFS_TEST_PATH="/ran/failover-test-${RUN_ID}"

docker exec "$HDFS_CLIENT" hdfs dfs -mkdir -p "$HDFS_TEST_PATH"
printf 'before-hdfs-failover %s\n' "$RUN_ID" | docker exec -i "$HDFS_CLIENT" \
  hdfs dfs -put -f - "${HDFS_TEST_PATH}/before.txt"

echo "Stopping Active ${ACTIVE_ID} (${ACTIVE_SERVICE})..."
HDFS_SCALED_SERVICE="$ACTIVE_SERVICE"
docker service scale "${STACK_NAME}_${ACTIVE_SERVICE}=0"
wait_replicas "$ACTIVE_SERVICE" 0/0 24 \
  || fail "${ACTIVE_SERVICE} did not stop."

NEW_STATE=""
for attempt in $(seq 1 36); do
  NEW_STATE="$(docker exec "$HDFS_CLIENT" \
    hdfs haadmin -getServiceState "$NEW_ACTIVE_ID" 2>/dev/null || true)"
  echo "Attempt ${attempt}: ${NEW_ACTIVE_ID}=${NEW_STATE}"
  [[ "$NEW_STATE" == "active" ]] && break
  sleep 5
done

[[ "$NEW_STATE" == "active" ]] \
  || fail "${NEW_ACTIVE_ID} did not become Active."

docker exec "$HDFS_CLIENT" \
  hdfs dfs -cat "${HDFS_TEST_PATH}/before.txt"

printf 'written-during-hdfs-failover %s\n' "$RUN_ID" | docker exec -i "$HDFS_CLIENT" \
  hdfs dfs -put -f - "${HDFS_TEST_PATH}/during.txt"

docker exec "$HDFS_CLIENT" \
  hdfs dfs -cat "${HDFS_TEST_PATH}/during.txt"

echo "Restoring ${ACTIVE_SERVICE}..."
docker service scale "${STACK_NAME}_${ACTIVE_SERVICE}=1"
wait_replicas "$ACTIVE_SERVICE" 1/1 48 \
  || fail "${ACTIVE_SERVICE} did not return to 1/1."
HDFS_SCALED_SERVICE=""

for _ in $(seq 1 36); do
  RETURNED_STATE="$(docker exec "$HDFS_CLIENT" \
    hdfs haadmin -getServiceState "$ACTIVE_ID" 2>/dev/null || true)"
  [[ "$RETURNED_STATE" == "standby" ]] && break
  sleep 5
done

docker exec "$HDFS_CLIENT" hdfs haadmin -getAllServiceState

FSCK_OUTPUT="$(docker exec "$HDFS_CLIENT" \
  hdfs fsck "$HDFS_TEST_PATH" -files -blocks -locations 2>&1)"
echo "$FSCK_OUTPUT" | tail -n 35
grep -q "is HEALTHY" <<<"$FSCK_OUTPUT" \
  || fail "HDFS test path is not healthy."

echo "HDFS automatic failover test PASSED."

echo
echo "============================================================"
echo "KAFKA AND HDFS FAILOVER TESTS PASSED"
echo "============================================================"

